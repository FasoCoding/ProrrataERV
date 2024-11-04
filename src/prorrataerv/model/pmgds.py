import polars as pl



def get_pmgd(path_pmgd: Path) -> pl.DataFrame:
    """Extracts a list of PMGDs from an Excel file on W disc.

    Returns:
        pl.DataFrame: A polars DataFrame with the list of PMGDs.
    """
    return pl.read_excel(
        source=path_pmgd.absolute(),
        sheet_name="Hoja1",
        xlsx2csv_options={"skip_empty_lines": True},
        read_csv_options={"new_columns": ["Nombre_CDC", "Centrales"]},
    )