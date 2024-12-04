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

def read_and_validate_file_from_hdfs(spark, input_file, expected_columns, error_handler, separator, encoding, limit):
    """
    Lee y valida el archivo desde HDFS, detectando errores y separando los registros válidos.
    :param spark: SparkSession
    :param input_file: Ruta del archivo en HDFS
    :param expected_columns: Número de columnas esperadas
    :param error_handler: Instancia de ErrorHandler
    :param separator: Separador del archivo
    :param encoding: Codificación del archivo
    :param limit: Límite de registros a leer ('all' para leer todos)
    :return: Lista de registros válidos, total de líneas procesadas, encabezado del archivo
    """
    print(f"Leyendo y validando archivo: {input_file} con separador '{separator}' y codificación '{encoding}'")
    valid_lines = []
    total_lines = 0

    df = spark.read.option("header", "true") \
        .option("sep", separator) \
        .option("encoding", encoding) \
        .csv(input_file)

    if limit != "all":
        df = df.limit(limit)

    for row in df.collect():
        total_lines += 1
        if len(row) == expected_columns:
            valid_lines.append(row)
        else:
            error_handler.handle_error(row)

    return valid_lines, total_lines, df.columns

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

def main():
    """Función principal que ejecuta el proceso ETL completo."""
    config = load_config(os.path.join('config', 'config.yaml'))
    
    max_parquet_file_size = config['output']['max_file_size_mb'] * 1024 * 1024  # Convertir MB a bytes
    records_to_process = config["records_to_process"]  # Leer el límite de registros desde la configuración
    
    total_records = 0
    total_valid_records = 0
    
    for source in config['sources']:
        if source['process']:  # Verifica si la fuente está activada para procesar
            print(f"Procesando fuente: {source['name']}")

            # Crear una sesión de Spark por cada fuente
            spark = SparkSession.builder \
                .appName(f"ETL_{source['name']}") \
                .config("spark.executorEnv.PYTHONPATH", os.path.join(os.getcwd(), 'src')) \
                .config("spark.driver.extraJavaOptions", f"-Xloggc:{config['spark']['log_gc_path']}") \
                .config("spark.sql.files.maxRecordsPerFile", max_parquet_file_size // 128) \
                .getOrCreate()

            error_file = source['error_file']
            encoding = source.get('encoding', 'utf-8')
            separator = source.get('separator', ',')  # Obtener el separador desde la configuración
            error_handler = ErrorHandler()

            all_valid_lines = []
            header_line = None
            for i, input_file in enumerate(source['input_files']):
                input_path = os.path.join(source['input_path'], input_file)
                valid_lines, total_lines, file_header = read_and_validate_file_from_hdfs(
                    spark, input_path, len(source['input_columns']), error_handler, separator, encoding, records_to_process
                )
                if i == 0:
                    header_line = file_header
                else:
                    # Validar que el encabezado sea el mismo
                    if header_line and file_header and header_line != file_header:
                        raise ValueError(f"El encabezado del archivo {input_file} no coincide con el del primer archivo.")
                total_records += total_lines
                all_valid_lines.extend(valid_lines)

            # Confirmar que los encabezados son correctos
            print("Encabezados validados y son correctos para todos los archivos de entrada.")

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
