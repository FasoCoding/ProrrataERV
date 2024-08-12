from dataclasses import dataclass
from pathlib import Path
from typing import List

import polars as pl

PATH_BANNED_GENERATORS = r"R:/Aplicaciones/Prorrateo_Vertimiento/Centrales_Vetadas.xlsx"

@dataclass
class BannedModel:
    path: Path
    centrales: List[str]

    @classmethod
    def from_excel(
        cls,
        path: str = PATH_BANNED_GENERATORS,
        sheet_pmgd: str = "Hoja1",
        column_pmgd: str = "A"
    ):
        path_banned = Path(path)
        if not (path_banned.exists() and path_banned.is_file()):
            raise ValueError(f"Path: {path_banned} does not exists.")
        return cls(
            path=path_banned,
            centrales=pl.read_excel(
                source=path_banned,
                sheet_name=sheet_pmgd,
                columns=column_pmgd,
            )
            .to_series(0)
            .to_list(),
        )
