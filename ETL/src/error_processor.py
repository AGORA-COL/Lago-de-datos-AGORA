"""
Código elaborado por: Pedro Fabian Perez Arteaga
Contacto: lideranalitica1@alianzacaoba.co
Descripción: Este script corrige los errores en los archivos de errores generados por el proceso ETL,
realiza la búsqueda y reemplazo de patrones específicos, valida los registros corregidos, y actualiza los archivos Parquet en HDFS.
"""

import yaml
import os
import sys
import time
from tqdm import tqdm
from pyspark.sql import SparkSession

# Agrega el directorio src al PYTHONPATH
sys.path.append(os.path.join(os.getcwd(), 'src'))

from etl.error_handler import ErrorHandler

def load_config(config_path):
    """Carga la configuración desde un archivo YAML."""
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def search_and_replace(line, pairs):
    """Realiza la búsqueda y reemplazo en una línea de texto."""
    for pair in pairs:
        line = line.replace(pair['search'], pair['replace'])
    return line

def read_and_correct_errors(error_file, pairs, expected_columns, encoding):
    """Lee el archivo de errores, realiza la corrección y valida los registros."""
    corrected_lines = []
    remaining_errors = []
    total_errors = 0

    with open(error_file, 'r', encoding=encoding) as ef:
        header = ef.readline().strip()
        for line in ef:
            total_errors += 1
            corrected_line = search_and_replace(line.strip(), pairs)
            columns = corrected_line.split(',')
            if len(columns) == expected_columns:
                corrected_lines.append(columns)
            else:
                remaining_errors.append(corrected_line)
    
    return corrected_lines, remaining_errors, total_errors, header

def write_corrected_parquet(spark, corrected_lines, output_path, columns):
    """Escribe los registros corregidos en un archivo Parquet."""
    df = spark.createDataFrame(corrected_lines, columns)
    with tqdm(total=len(corrected_lines), unit='records', desc="Escribiendo Parquet") as pbar:
        for partition in df.rdd.glom().collect():
            pbar.update(len(partition))
        df.write.mode('append').parquet(output_path)

def write_remaining_errors(remaining_errors, new_error_file, header, encoding):
    """Escribe los errores restantes en un nuevo archivo de errores."""
    total_remaining_errors = len(remaining_errors)

    if total_remaining_errors > 0:
        with open(new_error_file, 'w', encoding=encoding) as ef:
            ef.write(header + '\n')
            for line in remaining_errors:
                ef.write(line + '\n')
        print(f"Total de registros erróneos restantes: {total_remaining_errors}")
    else:
        if os.path.exists(new_error_file):
            os.remove(new_error_file)
        print("No quedan registros erróneos. Archivo de errores eliminado.")

def main():
    """Función principal que ejecuta el proceso de corrección de errores."""
    config = load_config(os.path.join('config', 'config_errors.yaml'))

    # Añade src al PYTHONPATH de Spark
    spark = SparkSession.builder \
        .appName("ErrorCorrection") \
        .config("spark.executorEnv.PYTHONPATH", os.path.join(os.getcwd(), 'src')) \
        .config("spark.driver.extraJavaOptions", f"-Xloggc:{config['spark']['log_gc_path']}") \
        .getOrCreate()

    source_config = config['error_correction']
    if source_config['process_errors']:  # Verifica si la fuente está activada para procesar errores
        print(f"Procesando corrección de errores para la fuente: {source_config['source']}")
        error_file = source_config['error_file']
        new_error_file = source_config['new_error_file']
        encoding = source_config.get('encoding')
        search_replace_pairs = source_config['replacements']
        expected_columns = len(source_config['input_columns'])
        output_path = source_config['corrected_output_path']

        corrected_lines, remaining_errors, total

    print(f"Total de registros erróneos leídos: {total_errors}")
    print(f"Total de registros corregidos: {len(corrected_lines)}")
    print(f"Total de registros restantes con errores: {len(remaining_errors)}")

    write_corrected_parquet(spark, corrected_lines, output_path, source_config['output_columns'])
    write_remaining_errors(remaining_errors, new_error_file, header, encoding)

    print(f"Archivos Parquet corregidos escritos en: {output_path}")
    print(f"Nuevo archivo de errores escrito en: {new_error_file}")

if name == “main”:
main()
