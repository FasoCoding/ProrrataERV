from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import (
    engine,
    create_engine,
)

import polars as pl


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


def get_access_data(sql_str: str, prg_engine: engine.Engine) -> pl.DataFrame:
    """Wrapper function to read data from a Microsoft Access database.

    Args:
        sql_str (str): sql query to be executed.
        prg_engine (engine.Engine): SQLAlchemy engine object.

    Raises:
        f: SQLAlchemyError if connection to database fails.

    Returns:
        pl.DataFrame: A polars DataFrame with the results of the query.
    """
    try:
        return pl.read_database(query=sql_str, connection=prg_engine)
    except SQLAlchemyError as e:
        raise f"Error: {e}"


def get_pmgd() -> pl.DataFrame:
    """Extracts a list of PMGDs from an Excel file on W disc.

    Returns:
        pl.DataFrame: A polars DataFrame with the list of PMGDs.
    """
    return pl.read_excel(
        source=Path(
            r"W:/41 Dpto Pronosticos/Vertimiento_ERNC/Lista_PMGDs.xlsx"
        ).absolute(),
        sheet_name="Hoja1",
        xlsx2csv_options={"skip_empty_lines": True},
        read_csv_options={"new_columns": ["Nombre_CDC", "Centrales"]},
    )


def get_banned_generators() -> pl.DataFrame:
    """Extracts a list of banned generators from an Excel file on R disc.

    Returns:
        pl.DataFrame: A polars DataFrame with the list of banned generators.
    """
    return pl.read_excel(
        source=Path(
            r"R:/Aplicaciones/Prorrateo_Vertimiento/Centrales_Vetadas.xlsx"
        ).absolute(),
        sheet_name="Hoja1",
        xlsx2csv_options={"skip_empty_lines": True},
        read_csv_options={"new_columns": ["Centrales"]},
    )
