"""
Código elaborado por: Pedro Fabian Perez Arteaga
Contacto: lideranalitica1@alianzacaoba.co
"""

from pyspark.sql import SparkSession

class DataReader:
    def __init__(self, spark, path):
        """Inicializa el lector de datos con la ruta del archivo."""
        self.path = path
        self.spark = spark

    def read(self, columns):
        """Lee el archivo CSV preprocesado y selecciona las columnas especificadas."""
        df = self.spark.read.option("header", "true").csv(self.path).select(*columns)
        return df

class DataReaderFactory:
    def __init__(self, spark):
        self.spark = spark

    def create_reader(self, path):
        """Crea una instancia de DataReader para la ruta proporcionada."""
        return DataReader(self.spark, path)
