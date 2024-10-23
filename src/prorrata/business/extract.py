# from prorrata.model.minstablelevel import MinStableLevelModel
import polars as pl
from prorrata.data import InputsModel
from dataclasses import dataclass


@dataclass
class ResultStruct:
    data: pl.DataFrame
    t_data_0: pl.DataFrame
    min_stable: pl.DataFrame


def process_inputs(path_pcp: str) -> ResultStruct:
    input_data = InputsModel.from_pcp(path_pcp)

    erv_pivot = clean_erv(input_data.erv, input_data.banned_list)
    inf_pivot = clean_inf(input_data.inf)
    min_stable = clean_mt(input_data.mt)

    data_pivot = pl.concat([erv_pivot, inf_pivot])
    t_data_0 = pl.concat([input_data.erv, input_data.inf])

    return ResultStruct(
        data=input_data.cmg.join(input_data.nodes, on="node", how="inner")
        .join(data_pivot, on=["generator", "datetime"], how="inner")
        .join(min_stable,on=["generator", "datetime"],how="left"),
        t_data_0=t_data_0,
        min_stable=min_stable,
    )


def clean_mt(data: pl.DataFrame) -> pl.DataFrame:
    return (
        data.with_columns(
            pl.datetime(
                year=pl.col("YEAR"),
                month=pl.col("MONTH"),
                day=pl.col("DAY"),
                hour=pl.col("PERIOD"),
            ).alias("datetime")
        )
        .filter(pl.col("NAME").str.contains("_GNL_INF"))
        .select(["NAME", "datetime", "VALUE"])
        .rename({"NAME": "generator", "VALUE": "MinTech"})
    )


def clean_erv(data: pl.DataFrame, filter_list: list[str]) -> pl.DataFrame:
    return (
        data.filter(
            ~pl.col("generator").is_in(filter_list),
        )
        .pivot(index=["generator", "datetime"], on="property", values="value")
        .filter(pl.col("Units Generating").eq(1))
        .select(pl.exclude("Units Generating"))
    )


def clean_inf(data: pl.DataFrame) -> pl.DataFrame:
    return (
        data.pivot(index=["generator", "datetime"], on="property", values="value")
        .filter(
            pl.col("generator").str.contains("GNL_INF"),
            pl.col("Available Capacity").gt(0),
            pl.col("Units Generating").eq(1),
        )
        .select(pl.exclude("Units Generating"))
    )


def check_inputs_health(inputs: ResultStruct) -> bool:
    total_row, _ = inputs.data.shape
    total_curtailment = inputs.data.select(pl.col("Capacity Curtailed").sum()).item(0, 0)

    return total_row > 0 and total_curtailment > 0
