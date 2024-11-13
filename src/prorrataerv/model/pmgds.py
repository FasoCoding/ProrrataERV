from pathlib import Path

import fastexcel

type PMGDNames = str

def get_pmgd_list(path_to_pmgd: Path) -> list[PMGDNames]:
    """recupera una lista de centrales vetadas del proceso de la prorrata.

    Args:
        path_to_banned (Path): ruta al excel con centrales pmgd.

    Returns:
        list[PMGDNames]: lista de nombres plexos
    """
    excel_reader = fastexcel.read_excel(path_to_pmgd.absolute())
    data_sheet = excel_reader.load_sheet_by_idx(0, use_columns="A")
    return data_sheet.to_polars().to_series().to_list()
