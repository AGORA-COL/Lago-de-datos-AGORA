"""
Código elaborado por: Pedro Fabian Perez Arteaga
Contacto: lideranalitica1@alianzacaoba.co
Descripción: Este script principal ejecuta el proceso ETL, incluyendo la lectura de archivos desde HDFS,
el procesamiento de registros, la detección de errores, y la escritura de datos procesados y errores en los destinos especificados.
"""

import yaml
import os
import sys
import time
from tqdm import tqdm
from pyspark.sql import SparkSession

# Agrega el directorio src al PYTHONPATH
sys.path.append(os.path.join(os.getcwd(), 'src'))

from etl.reader import DataReaderFactory
from etl.writer import DataWriter
from etl.transformer import DataTransformer
from etl.error_handler import ErrorHandler

def load_config(config_path):
    """Carga la configuración desde un archivo YAML."""
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def read_and_validate_file_from_hdfs(spark, input_file, expected_columns, error_handler, is_first_file):
    """
    Lee y valida el archivo desde HDFS en el controlador, detectando errores y separando los registros válidos.
    Si `is_first_file` es True, elimina la primera línea del archivo como encabezado.
    """
    print(f"Leyendo y validando archivo: {input_file}")
    valid_lines = []
    total_lines = 0

    rdd = spark.sparkContext.textFile(input_file)
    if is_first_file:
        header_line = rdd.first()
        rdd = rdd.filter(lambda line: line != header_line)
    else:
        header_line = None

    for line in rdd.collect():
        total_lines += 1
        columns = line.split(',')
        if len(columns) == expected_columns:
            valid_lines.append(columns)
        else:
            error_handler.handle_error(line)

    return valid_lines, total_lines, header_line

def write_to_parquet(spark, valid_lines, output_path, columns):
    """
    Escribe las líneas válidas en un archivo Parquet usando Spark.
    """
    df = spark.createDataFrame(valid_lines, columns)
    # Escribir con una barra de progreso
    with tqdm(total=len(valid_lines), unit='records', desc="Escribiendo Parquet") as pbar:
        for partition in df.rdd.glom().collect():
            pbar.update(len(partition))
        df.write.mode('overwrite').parquet(output_path)

def write_errors_to_local(errors, error_file, header, encoding):
    """Escribe los errores acumulados en un archivo local con salida de porcentaje de avance."""
    os.makedirs(os.path.dirname(error_file), exist_ok=True)

    # Eliminar el archivo de errores si ya existe
    if os.path.exists(error_file):
        os.remove(error_file)

    total_errors = len(errors)
    start_time = time.time()

    with open(error_file, 'w', encoding=encoding, errors='replace') as ef:
        # Escribir el encabezado en el archivo de errores
        ef.write(header + '\n')
        for i, line in enumerate(errors):
            ef.write(line + '\n')
            if (i + 1) % (total_errors // 4) == 0:  # Actualizar cada 25% de progreso
                elapsed_time = time.time() - start_time
                progress = (i + 1) / total_errors * 100
                speed = (i + 1) / elapsed_time
                estimated_time = elapsed_time / (i + 1) * (total_errors - (i + 1))
                print(f"\rProgreso: {progress:.2f}% - Escrito: {i+1}/{total_errors} - Velocidad: {speed:.2f} líneas/seg - Tiempo estimado restante: {estimated_time:.2f} seg", end='')

    print("\nProcesamiento de errores completado.")

def clean_output_directory(spark, output_path):
    """Limpia el directorio de salida en HDFS."""
    hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    output_path_hdfs = spark._jvm.org.apache.hadoop.fs.Path(output_path)
    if hadoop_fs.exists(output_path_hdfs):
        print(f"Limpiando el directorio de salida: {output_path}")
        hadoop_fs.delete(output_path_hdfs, True)

def main():
    """Función principal que ejecuta el proceso ETL completo."""
    config = load_config(os.path.join('config', 'config.yaml'))
    
    max_parquet_file_size = config['output']['max_file_size_mb'] * 1024 * 1024  # Convertir MB a bytes

    # Añade src al PYTHONPATH de Spark
    spark = SparkSession.builder \
        .appName("ETL") \
        .config("spark.executorEnv.PYTHONPATH", os.path.join(os.getcwd(), 'src')) \
        .config("spark.driver.extraJavaOptions", f"-Xloggc:{config['spark']['log_gc_path']}") \
        .config("spark.sql.files.maxRecordsPerFile", max_parquet_file_size // 128) \
        .getOrCreate()
    
    total_records = 0
    total_valid_records = 0
    
    for source in config['sources']:
        if source['process']:  # Verifica si la fuente está activada para procesar
            print(f"Procesando fuente: {source['name']}")
            error_file = source['error_file']
            encoding = source.get('encoding', 'utf-8')
            error_handler = ErrorHandler()

            all_valid_lines = []
            header_line = None
            for i, input_file in enumerate(source['input_files']):
                input_path = os.path.join(source['input_path'], input_file)
                valid_lines, total_lines, file_header = read_and_validate_file_from_hdfs(spark, input_path, len(source['input_columns']), error_handler, is_first_file=(i == 0))
                if i == 0:
                    header_line = file_header
                else:
                    # Validar que el encabezado sea el mismo
                    if header_line and file_header and header_line != file_header:
                        raise ValueError(f"El encabezado del archivo {input_file} no coincide con el del primer archivo.")
                total_records += total_lines
                all_valid_lines.extend(valid_lines)
            
            # Escribir errores al final del proceso en el controlador
            if error_handler.get_errors():
                print(f"Escribiendo errores a: {error_file}")
                write_errors_to_local(error_handler.get_errors(), error_file, header_line, encoding)
            
            # Limpiar el directorio de salida antes de escribir los nuevos datos
            output_path = source['output_path']
            clean_output_directory(spark, output_path)

            # Transformar y escribir los datos válidos en Parquet
            if all_valid_lines:
                write_to_parquet(spark, all_valid_lines, output_path, source['output_columns'])
                total_valid_records += len(all_valid_lines)
    
    print(f"Total de registros leídos: {total_records}")
    print(f"Total de registros válidos: {total_valid_records}")
    print(f"Total de registros erróneos: {error_handler.get_error_count()}")

if __name__ == "__main__":
    main()
