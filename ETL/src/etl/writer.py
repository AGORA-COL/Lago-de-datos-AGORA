"""
Código elaborado por: Pedro Fabian Perez Arteaga
Contacto: lideranalitica1@alianzacaoba.co
"""

import os

class DataWriter:
    def __init__(self, max_file_size_mb):
        """Inicializa el escritor de datos con el tamaño máximo de archivo."""
        self.max_file_size_mb = max_file_size_mb

    def write(self, df, output_path):
        """Escribe el DataFrame en la ruta de salida especificada."""
        df.write.option("maxRecordsPerFile", self.max_file_size_mb * 1024 * 1024).parquet(output_path)
