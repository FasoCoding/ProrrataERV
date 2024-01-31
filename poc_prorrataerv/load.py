from pathlib import Path
from typing import Protocol

#from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.expression import (
    table,
    column,
    update
)
from sqlalchemy import (
    engine,
    create_engine,
)

import polars as pl

PATH_ACCDB_OUTPUT = r"Antecedentes/Model PRGdia_Full_Definitivo Solution.accdb"

class DataProcessor(Protocol):
    t_data_0: pl.LazyFrame

class DataLoader:
    path_prg: Path

    def __init__(self, path_prg: str):
        temp_path = Path(path_prg)
        if temp_path.exists() and temp_path.joinpath(PATH_ACCDB_OUTPUT).exists():
            self.path_prg = temp_path
        else:
            raise ValueError(f"Path: {path_prg} does not exists.")
    
    def load_data(self, data_processor: DataProcessor) -> None:
        """Inicia proceso de carga de datos.
        """
        t_data_0 = table("t_data_0", column("key_id"), column("period_id"), column("value"))

        with create_prg_engine(self.path_prg).begin() as conn:
            for row in data_processor.t_data_0.collect().to_dicts():
                stmt = (
                    update(t_data_0)
                    .where(
                        t_data_0.c.key_id == row.get("key_id"),
                        t_data_0.c.period_id == row.get("period_id")
                    )
                    .values(value=row.get("value"))
                )
                conn.execute(stmt)
            #conn.commit()

def create_prg_engine(path_prg: Path) -> engine.Engine:
    """Creates a SQLAlchemy engine for a Microsoft Access database.

    This function takes a path to a Microsoft Access database file and returns a SQLAlchemy engine
    that can be used to interact with the database.

    Args:
        path_prg (Path): A pathlib.Path object representing the path to the .mdb or .accdb file.

    Raises:
        ValueError: If the provided path does not exist.

    Returns:
        engine.Engine: A SQLAlchemy engine object.
    """
    if not path_prg.exists():
        raise ValueError(f"Path: {path_prg} does not exists.")

    connection_string = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={path_prg.as_posix()};"
        r"ExtendedAnsiSQL=1;"
    )
    connection_url = engine.URL.create(
        "access+pyodbc", query={"odbc_connect": connection_string}
    )

    return create_engine(connection_url)