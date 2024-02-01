"""
__init__.py
Lectura de archivos SQL y creacion de variables para su uso en el proyecto.
"""
from pathlib import Path

# Lectura de SQL Barra-Generador
PATH_SQL_NODE = Path(r"poc_prorrataerv/sql/gen_node.sql").absolute()
PATH_SQL_GEN = Path(r"poc_prorrataerv/sql/gen_data.sql").absolute()
PATH_SQL_CMG = Path(r"poc_prorrataerv/sql/cmg_data.sql").absolute()

def read_sql(path: str) -> str:
    """Lee un archivo SQL y retorna el contenido como un string.

    Args:
        path (str): Ruta al archivo SQL.

    Returns:
        str: Contenido del archivo SQL.
    """
    with open(path, "r") as file:
        return file.read()
    
class SQL:
    """Clase para leer archivos SQL y crear variables para su uso en el proyecto."""
    def __init__(self):
        self.sql_node = read_sql(PATH_SQL_NODE)
        self.sql_gen = read_sql(PATH_SQL_GEN)
        self.sql_cmg = read_sql(PATH_SQL_CMG)