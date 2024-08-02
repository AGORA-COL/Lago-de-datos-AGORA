"""
Código elaborado por: Pedro Fabian Perez Arteaga
Contacto: lideranalitica1@alianzacaoba.co
"""

class DataTransformer:
    def __init__(self, column_renames):
        """Inicializa el transformador de datos con las reglas de renombrado."""
        self.column_renames = column_renames

    def rename_columns(self, df):
        """Renombra las columnas del DataFrame según las reglas especificadas."""
        for rename in self.column_renames:
            df = df.withColumnRenamed(rename['original'], rename['new'])
        return df
