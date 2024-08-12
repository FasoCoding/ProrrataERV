from dataclasses import dataclass
from pathlib import Path

import polars as pl

PATH_CSV = r"Datos\Gen_MinStableLevel.csv"

@dataclass
class MinStableLevelModel:
    path: Path
    data: pl.DataFrame

    @classmethod
    def from_csv(cls, path_pcp: str):
        path = Path(path_pcp) / PATH_CSV
        if not (path.exists() and path.is_file()):
            raise ValueError(f"Path: {path} does not exists.")
        
        return cls(
            path=path,
            data=pl.read_csv(path)
        )
