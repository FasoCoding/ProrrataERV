from pathlib import Path
from typing import Protocol

import pyodbc
import polars as pl

PATH_ACCDB_OUTPUT = r"Antecedentes/Model PRGdia_Full_Definitivo Solution.accdb"

class DataProcessor(Protocol):
    t_data_0: pl.LazyFrame

class DataLoader:
    path_prg: Path

    def __init__(self, path_prg: str):
        temp_path = Path(path_prg)
        if temp_path.exists() and temp_path.joinpath(PATH_ACCDB_OUTPUT).exists():
            self.path_prg = temp_path.joinpath(PATH_ACCDB_OUTPUT)
        else:
            raise ValueError(f"Path: {path_prg} does not exists.")
    
    def load_data(self, data_processor: DataProcessor) -> None:
        """Inicia proceso de carga de datos.
        """
        connection_string = (
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            rf"DBQ={self.path_prg.as_posix()};"
            r"ExtendedAnsiSQL=1;"
        )

        cnxn = pyodbc.connect(connection_string)
        crsr = cnxn.cursor()

        try:
            cnxn.autocommit = False
            params = [(data['value'],data['key_id'],data['period_id']) for data in data_processor.t_data_0.collect().to_dicts()]
            crsr.executemany("UPDATE t_data_0 SET t_data_0.value = ? WHERE t_data_0.key_id = ? AND t_data_0.period_id = ?;", params)
        except pyodbc.DatabaseError as err:
            cnxn.rollback()
            print(f"Error: {err}")
        else:
            cnxn.commit()
        finally:
            cnxn.close()
