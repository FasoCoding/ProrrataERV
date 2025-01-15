from dataclasses import dataclass
from typing import Optional

import polars as pl

from prorrata.extract import DataExtractor

WEIGHT_COL = "Max Capacity"
DATETIME_COL = "datetime"
GENERATION_COL = "Generation"
PRORRATA_COL = "Prorrata"
ERROR_COL = "Error"
AVAILABLE_COL = "Available Capacity"
ACTIVE_COL = "Active"

@dataclass
class DataProcessor:
    data: pl.DataFrame
    t_data_0: Optional[pl.DataFrame]

    @classmethod
    def from_extractor(cls, data_extractor: DataExtractor):
        return cls(
            data = _join_data(data_extractor),
            t_data_0 = None
        )

    def process_prorrata(self):
        self.data = _create_prorrata(data= self.data)
        self.data = _calc_error(self.data)
        self.data = _process_prorrata(data= self.data)
    
    def get_t_data(self, data_extractor: DataExtractor) -> None:
        self.t_data_0 = _t_data_update(self.data, data_extractor)
    
    def show_results(self) -> pl.DataFrame:
        return (
            self.data
            .group_by("datetime")
            .agg(
                (pl.col("Prorrata").sum() - pl.col("Generation").sum()).alias("Error_Prorrata"),
                pl.col("Capacity Curtailed").sum().alias("Total_Curtailed"),
            )
            .sort(by="datetime")
            #.collect()
        )

def _t_data_update(df: pl.DataFrame, data_extractor: DataExtractor) -> pl.DataFrame:
    return (
        df
        .join(
            data_extractor.gen.filter(pl.col("property") == "Generation"),#.lazy(),
            on=["generator","datetime"],
            how="inner"
        )
        .select(
            pl.col("data_key").alias("key_id"),
            pl.col("data_period").alias("period_id"),
            pl.col("Prorrata").alias("value"),
        )
        .sort(by=["key_id","period_id"])
        #.lazy()
    )

def _join_data(data_extractor: DataExtractor) -> pl.DataFrame:
    """
    Une toda la información extraida en 1 dataframe.
    """
    gen_data_pivot = (
        data_extractor.gen
        .filter(
            ~pl.col("generator").is_in(data_extractor.banned["Centrales"].unique()),
            ~pl.col("generator").is_in(data_extractor.pmgd["Centrales"].unique()),
        )
        .pivot(values="value", columns="property", index=["generator", "datetime"])
        .filter(pl.col("Units Generating") == 1)
        .select(pl.exclude("Units Generating"))
    )

    return (
        data_extractor.cmg
        .join(data_extractor.nodes, on="node", how="inner")
        .join(gen_data_pivot, on=["generator", "datetime"], how="inner")
        #.lazy()
    )

def _create_prorrata(data: pl.DataFrame) -> pl.DataFrame:
    return (
        data.with_columns(
            (
                pl.col(GENERATION_COL).sum().over(DATETIME_COL)
                * pl.col(WEIGHT_COL)
                / pl.col(WEIGHT_COL).sum().over(DATETIME_COL)
            ).alias(PRORRATA_COL),
            pl.lit(0).alias(ERROR_COL)
        )
    )


def _process_prorrata(data: pl.DataFrame) -> pl.DataFrame:
    data_processed = _calc_new_prorrata(data)
    data_processed = _calc_error(data_processed)

    if _check_error(data_processed):
        return _process_prorrata(data_processed)
    
    return data_processed

def _calc_new_prorrata(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (
            pl.col(PRORRATA_COL)
            + pl.col(ERROR_COL).sum().over(DATETIME_COL)
            * pl.col(WEIGHT_COL)
            / pl.col(WEIGHT_COL).sum().over(DATETIME_COL)
        ).alias(PRORRATA_COL),
    )

def _calc_error(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col(PRORRATA_COL).gt(pl.col(AVAILABLE_COL)))
        .then(pl.col(PRORRATA_COL) - pl.col(AVAILABLE_COL))
        .otherwise(0)
        .alias(ERROR_COL),

        pl.when(pl.col(PRORRATA_COL).gt(pl.col(AVAILABLE_COL)))
        .then(pl.col(AVAILABLE_COL))
        .otherwise(pl.col(PRORRATA_COL))
        .alias(PRORRATA_COL),
    )

def _check_error(df: pl.DataFrame, tol: float = 1e-1) -> bool:
    total_error = df.select(pl.col(ERROR_COL).sum()).item()
    unit_error = df.select(pl.col(ERROR_COL).ge(tol).any()).item()
    return unit_error or (total_error < 5)
    
    #print(f"error: {df.select(pl.col(ERROR_COL).sum()).collect().item()}")
    #return df.select(pl.col(ERROR_COL).ge(tol).any()).collect().item()
