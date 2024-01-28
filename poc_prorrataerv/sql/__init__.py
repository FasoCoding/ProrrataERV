"""
__init__.py
Lectura de archivos SQL y creacion de variables para su uso en el proyecto.
"""
from pathlib import Path

# Lectura de SQL Barra-Generador
path_sql_node = Path(r"poc_prorrataerv/sql/gen_node.sql").absolute()
with open(path_sql_node, "r") as file:
    sql_node = file.read()

# Lectura de SQL con data de generacion
path_sql_gen = Path(r"poc_prorrataerv/sql/gen_data.sql").absolute()
with open(path_sql_gen, "r") as file:
    sql_gen = file.read()

# lectura de SQL con data de barras con costos marginales menor a 0
path_sql_cmg = Path(r"poc_prorrataerv/sql/cmg_data.sql").absolute()
with open(path_sql_cmg, "r") as file:
    sql_cmg = file.read()