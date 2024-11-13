from pathlib import Path
from typing import TypeAlias

import fastexcel

BannedNames: TypeAlias = str


def get_banned_list(path_to_banned: Path) -> list[BannedNames]:
    """recupera una lista de centrales vetadas del proceso de la prorrata.

    Args:
        path_to_banned (Path): ruta al excel con centrales vetadas.

    Returns:
        list[BannedNames]: lista de nombres plexos
    """
    excel_reader = fastexcel.read_excel(path_to_banned.absolute())
    data_sheet = excel_reader.load_sheet_by_idx(0, use_columns="A")
    return data_sheet.to_polars().to_series().to_list()
