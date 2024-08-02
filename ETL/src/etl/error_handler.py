"""
Código elaborado por: Pedro Fabian Perez Arteaga
Contacto: lideranalitica1@alianzacaoba.co
Descripción: Este módulo maneja los errores encontrados durante el procesamiento de datos.
"""

class ErrorHandler:
    def __init__(self):
        """Inicializa el manejador de errores."""
        self.errors = []
        self.error_count = 0

    def handle_error(self, line):
        """Maneja las líneas con más columnas de las esperadas y las agrega a la lista de errores."""
        self.errors.append(line)
        self.error_count += 1

    def get_error_count(self):
        """Devuelve el número de errores encontrados."""
        return self.error_count

    def get_errors(self):
        """Devuelve la lista de errores."""
        return self.errors
