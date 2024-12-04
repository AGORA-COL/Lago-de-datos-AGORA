"""
Código elaborado por: Pedro Fabian Perez Arteaga
Contacto: lideranalitica1@alianzacaoba.co
"""

from pyspark.sql import SparkSession

class DataReader:
    def __init__(self, spark, path, separator, encoding):
        """
        Inicializa el lector de datos con la ruta del archivo, separador y codificación.
        :param spark: SparkSession
        :param path: Ruta del archivo
        :param separator: Separador de columnas
        :param encoding: Codificación del archivo
        """
        self.path = path
        self.separator = separator
        self.encoding = encoding
        self.spark = spark

    def read(self, columns, limit=None):
        """
        Lee el archivo CSV con las opciones especificadas y selecciona las columnas deseadas.
        :param columns: Columnas a seleccionar
        :param limit: Límite de registros a leer (None para leer todo)
        :return: DataFrame filtrado
        """
        df = self.spark.read.option("header", "true") \
            .option("sep", self.separator) \
            .option("encoding", self.encoding) \
            .csv(self.path) \
            .select(*columns)
        
        if limit and limit != "all":
            df = df.limit(limit)
        return df

class DataReaderFactory:
    def __init__(self, spark):
        self.spark = spark

    def create_reader(self, path, separator=",", encoding="utf-8"):
        """
        Crea una instancia de DataReader para la ruta, separador y codificación proporcionados.
        :param path: Ruta del archivo
        :param separator: Separador de columnas
        :param encoding: Codificación del archivo
        :return: DataReader
        """
        return DataReader(self.spark, path, separator, encoding)
