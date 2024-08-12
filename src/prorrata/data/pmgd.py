from dataclasses import dataclass
from pathlib import Path
from typing import List

import polars as pl

PATH_PMGD_EXCLUDE =  r"W:/41 Dpto Pronosticos/Vertimiento_ERNC/Lista_PMGDs.xlsx"

@dataclass
class PMGDsModel:
    path: Path
    centrales: List[str]

    @classmethod
    def from_excel(
        cls,
        path: str = PATH_PMGD_EXCLUDE,
        sheet_pmgd: str = "Hoja1",
        column_pmgd: str = "B"
    ):
        path_pmgd = Path(path)
        if not (path_pmgd.exists() and path_pmgd.is_file()):
            raise ValueError(f"Path: {path} does not exists.")
        return cls(
            path=path_pmgd,
            centrales=pl.read_excel(
                source=path_pmgd,
                sheet_name=sheet_pmgd,
                columns=column_pmgd,
            )
            .to_series(0)
            .to_list(),
        )
