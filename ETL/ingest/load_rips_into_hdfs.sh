#!/usr/bin/env bash
set -euo pipefail

# Config
SRC_DIR="${1:-$PWD}"                   # default: current directory; or pass /path/to/rawdatatemp
HDFS_DIR="/agora/raw/RIPS"

# Helpers
ts() { date +"%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(ts)] $*"; }
err() { echo "[$(ts)] ERROR: $*" >&2; }

# Ensure HDFS destination exists
log "Ensuring HDFS path exists: ${HDFS_DIR}"
hdfs dfs -mkdir -p "${HDFS_DIR}"

shopt -s nullglob
zip_files=("${SRC_DIR}"/RIPS_*.zip)

if (( ${#zip_files[@]} == 0 )); then
  log "No ZIP files found in ${SRC_DIR}. Nothing to do."
  exit 0
fi

for zipf in "${zip_files[@]}"; do
  log "Processing: $(basename "$zipf")"

  # List .txt entries inside ZIP (there should be one RIPS txt)
  mapfile -t txt_entries < <(unzip -Z1 "$zipf" | grep -Ei '\.txt$' || true)

  if (( ${#txt_entries[@]} == 0 )); then
    err "No .txt file found inside $(basename "$zipf"); skipping."
    continue
  fi
  if (( ${#txt_entries[@]} > 1 )); then
    log "Found ${#txt_entries[@]} TXT files; will process the first:"
    printf '  - %s\n' "${txt_entries[@]}"
  fi

  inner_txt="${txt_entries[0]}"
  base_txt="$(basename "$inner_txt")"
  hdfs_target="${HDFS_DIR}/${base_txt}"

  # Skip if already exists in HDFS
  if hdfs dfs -test -f "${hdfs_target}"; then
    log "HDFS already has ${base_txt}; skipping upload."
    continue
  fi

  # Extract the TXT stream to a secure temp file (no full unzip to disk)
  tmp_txt="$(mktemp --suffix=".txt" "/tmp/${base_txt}.XXXXXXXX")"
  cleanup_tmp() { rm -f -- "$tmp_txt"; }
  trap cleanup_tmp EXIT

  log "Extracting ${inner_txt} from ZIP to temp file..."
  if ! unzip -p "$zipf" "$inner_txt" > "$tmp_txt"; then
    err "Failed to extract ${inner_txt} from $(basename "$zipf"); skipping."
    cleanup_tmp
    trap - EXIT
    continue
  fi

  # Quick sanity check: non-empty
  if ! [ -s "$tmp_txt" ]; then
    err "Extracted file is empty (${base_txt}); skipping."
    cleanup_tmp
    trap - EXIT
    continue
  fi

  # Upload
  log "Uploading ${base_txt} to HDFS ${HDFS_DIR} ..."
  if ! hdfs dfs -put -f "$tmp_txt" "${hdfs_target}"; then
    err "hdfs dfs -put failed for ${base_txt}; leaving ZIP for later retry."
    cleanup_tmp
    trap - EXIT
    continue
  fi

  # Validate upload (exists and non-zero size in HDFS)
  if ! hdfs dfs -test -f "${hdfs_target}"; then
    err "Upload validation failed: ${base_txt} not found in HDFS."
    cleanup_tmp
    trap - EXIT
    continue
  fi

  hdfs_bytes="$(hdfs dfs -stat "%b" "${hdfs_target}")" || hdfs_bytes=0
  if [[ "${hdfs_bytes}" -le 0 ]]; then
    err "Upload validation failed: ${base_txt} has 0 bytes in HDFS."
    cleanup_tmp
    trap - EXIT
    continue
  fi

  log "Upload verified (${base_txt}, ${hdfs_bytes} bytes). Cleaning up local files..."
  # Remove the temp file and the original ZIP after success
  cleanup_tmp
  trap - EXIT
  rm -f -- "$zipf"

  log "Done with ${base_txt}."
done

log "All done."
