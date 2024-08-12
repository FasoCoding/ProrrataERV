# from prorrata.model.minstablelevel import MinStableLevelModel
import polars as pl
from prorrata.data import InputsModel


def process_inputs(path_pcp: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    input_data = InputsModel.from_pcp(path_pcp)

    erv_pivot = clean_erv(input_data.erv, input_data.banned_list)
    inf_pivot = clean_inf(input_data.inf)

    data_pivot = pl.concat([erv_pivot, inf_pivot])    
    t_data_0 = pl.concat([input_data.erv, input_data.inf])
    min_stable = input_data.mt

    return input_data.cmg.join(input_data.nodes, on="node", how="inner").join(
        data_pivot, on=["generator", "datetime"], how="inner"
    ), t_data_0, min_stable


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


def check_inputs_health(data: pl.DataFrame) -> bool:
    total_row, _ = data.shape
    total_curtailment = data.select(pl.col("Capacity Curtailed").sum()).item(0, 0)

    return total_row > 0 and total_curtailment > 0
