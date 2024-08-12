from dataclasses import dataclass
from pathlib import Path

import pyodbc
import polars as pl
import importlib.resources as sql_resources

PATH_ACCDB_INPUT = r"Datos/Model PRGdia_Full_Definitivo Solution/Model PRGdia_Full_Definitivo Solution.accdb"
PATH_ACCDB_OUTPUT = r"Antecedentes/Model PRGdia_Full_Definitivo Solution.accdb"

@dataclass
class PlexosExtractModel:
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

@dataclass
class DataLoaderModel:
    path: Path
    param_list: list[dict[str, int|float]]


    def load_data(self) -> None:
        """Inicia proceso de carga de datos.
        """
        path_accdb = self.path / PATH_ACCDB_OUTPUT
        if not (path_accdb.exists() and path_accdb.is_file()):
            raise ValueError(f"Path: {path_accdb} does not exists.")

        connection_string = (
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            rf"DBQ={path_accdb.as_posix()};"
            r"ExtendedAnsiSQL=1;"
        )

        cnxn = pyodbc.connect(connection_string)
        crsr = cnxn.cursor()

        try:
            cnxn.autocommit = False
            params = [(data['value'],data['key_id'],data['period_id']) for data in self.param_list]
            crsr.executemany("UPDATE t_data_0 SET t_data_0.value = ? WHERE t_data_0.key_id = ? AND t_data_0.period_id = ?;", params)
        except pyodbc.DatabaseError as err:
            cnxn.rollback()
            print(f"Error: {err}")
        else:
            cnxn.commit()
        finally:
            cnxn.close()
