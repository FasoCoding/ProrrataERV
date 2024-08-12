from dataclasses import dataclass
from pathlib import Path

import pyodbc
import polars as pl
import importlib.resources as sql_resources

PATH_ACCDB_INPUT = r"Datos/Model PRGdia_Full_Definitivo Solution/Model PRGdia_Full_Definitivo Solution.accdb"

@dataclass
class AccdbInputsModel:
    path: Path
    erv: pl.DataFrame
    cmg: pl.DataFrame
    nodes: pl.DataFrame
    inf: pl.DataFrame

    @classmethod
    def from_accdb(
        cls, path_pcp: str,
    ):
        path_accdb = Path(path_pcp) / PATH_ACCDB_INPUT
        if not (path_accdb.exists() and path_accdb.is_file()):
            raise ValueError(f"Path: {path_accdb} does not exists.")
        connection_string = (
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            rf"DBQ={path_accdb.as_posix()};"
            r"ExtendedAnsiSQL=1;"
        )
        sql = sql_resources.files("prorrata.utils")
        sql_nodes = (sql / "gen_node.sql").read_text()
        sql_erv = (sql / "gen_data.sql").read_text()
        sql_cmg = (sql / "cmg_data.sql").read_text()
        sql_inf = (sql / "inf_data.sql").read_text()

        try:
            conn = pyodbc.connect(connection_string)

            erv_data = pl.read_database(query=sql_erv, connection=conn)
            cmg_data = pl.read_database(query=sql_cmg, connection=conn)
            nodes_data = pl.read_database(query=sql_nodes, connection=conn)
            inf_data = pl.read_database(query=sql_inf, connection=conn)

        except pyodbc.Error as e:
            #logging.error(f"Error en la carga de datos desde el ACCDB {e}")
            print(f"Error en la carga de datos desde el ACCDB {e}")
        
        finally:
            conn.close()
        
        return cls(
            path=path_accdb,
            erv=erv_data,
            cmg=cmg_data,
            nodes=nodes_data,
            inf=inf_data
        )
