from pathlib import Path

import fastexcel

type banned_names = str

def get_banned_list(path_to_banned: Path) -> list[banned_names]:
    """recupera una lista de centrales vetadas del proceso de la prorrata.

    Args:
        path_to_banned (Path): ruta al excel con centrales vetadas.

    Returns:
        list[str]: lista de nombres plexos
    """
    excel_reader = fastexcel.read_excel(path_to_banned.absolute())
    data_sheet = excel_reader.load_sheet_by_idx(0, use_columns="A")
    return data_sheet.to_polars().to_series().to_list()
