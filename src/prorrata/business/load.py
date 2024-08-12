import polars as pl
from pathlib import Path
from prorrata.data.plexos import DataLoaderModel

def load_data(path: str, t_data_new: pl.DataFrame) -> None:
    
    loader = DataLoaderModel(
        Path(path), t_data_new.to_dicts()
    )

    loader.load_data()

