import polars as pl

from typing import Protocol

class DataExtractor(Protocol):
    nodes: pl.DataFrame
    gen: pl.DataFrame
    cmg: pl.DataFrame
    pmgd: pl.DataFrame
    banned: pl.DataFrame

class DataProcessor:
    data: pl.LazyFrame
    t_data_0: pl.LazyFrame

    def __init__(self, data_extractor: DataExtractor):
        self.data = _join_data(data_extractor)
    
    def process_prorrata(self):
        self.data = _process_prorrata(self.data, "Available Capacity", "Capacity Curtailed")
    
    def show_results(self) -> pl.DataFrame:
        """Generates a summary of the results for the prorate calculation.

        Args:
            df (pl.LazyFrame): Complete dataset with the prorate calculation.

        Returns:
            pl.DataFrame:  Summary of the results for the prorate calculation.
        """
        return (
            self.data
            .group_by("datetime")
            .agg(
                #pl.col("Generation").sum().alias("Total_Gen"),
                #pl.col("Prorrata").sum().alias("Total_Gen_Prorrata"),
                (pl.col("Prorrata").sum() - pl.col("Generation").sum()).alias("Error_Prorrata"),
                pl.col("Capacity Curtailed").sum().alias("Total_Curtailed"),
            )
            .sort(by="datetime")
            .collect()
        )
    
    def get_t_data(self, data_extractor: DataExtractor) -> None:
        self.t_data_0 = _t_data_update(self.data, data_extractor)
    
def _t_data_update(df: pl.LazyFrame, data_extractor: DataExtractor) -> pl.LazyFrame:
    return (
        df
        .join(
            data_extractor.gen.filter(pl.col("property") == "Generation").lazy(),
            on=["generator","datetime"],
            how="inner"
        )
        .select(
            pl.col("data_key").alias("key_id"),
            pl.col("data_period").alias("period_id"),
            pl.col("Prorrata").alias("value"),
        )
        .sort(by=["key_id","period_id"])
    )


def _join_data(data_extractor: DataExtractor) -> pl.LazyFrame:
    gen_pivot = _pivot_gen(data_extractor.gen, data_extractor.banned, data_extractor.pmgd)
    return (
        data_extractor.cmg
        .join(
            data_extractor.nodes,
            on="node",
            how="inner")
        .join(
            gen_pivot,
            on=["generator", "datetime"],
            how="inner"
        )
        .lazy()
    )

def _calc_error(
    df: pl.LazyFrame, error_target: str = "Prorrata", error_col_name: str = "Error"
) -> pl.LazyFrame:
    """Calculates de error column for the prorate calculation. If the prorate is negative, the error is the absolute value of the prorate.
    Aditionally, the prorate is set to 0 if it is negative.

    Args:
        df (pl.LazyFrame): original data for prorate calculation.
        error_target (str, optional): Target column to calculate the error. Defaults to "Prorrata".
        error_col_name (str, optional): Name for Error column. Defaults to "Error".

    Returns:
        pl.LazyFrame: Data with new columns for error and prorate.
    """
    return df.with_columns(
        pl.when(pl.col(error_target).lt(0))
        .then(pl.col(error_target).abs())
        .otherwise(0)
        .alias(error_col_name),
        pl.when(pl.col(error_target).lt(0))
        .then(0)
        .otherwise(pl.col(error_target))
        .alias(error_target),
    )


def _check_error(df: pl.LazyFrame, error_col: str = "Error", tol: float = 1e-3) -> bool:
    """Calculate the total error and check if it is greater than the tolerance.
    This calculation is done over every hour and set to true if any of the hours has an error greater than the tolerance.

    Args:
        df (pl.LazyFrame): Original data for prorate calculation.
        error_col (str, optional): Column to calculate error for. Defaults to "Error".
        tol (float, optional): Set tolerance for error. Defaults to 1e-3.

    Returns:
        bool: True if the error on any hour is greater than the tolerance. False otherwise.
    """
    return df.select(pl.col(error_col).ge(tol).any()).collect().item()


def _show_total_error(df: pl.LazyFrame, error_col: str = "Error") -> float:
    """Calculate the total error as the sum of the error column.

    Args:
        df (pl.LazyFrame): Original data for prorate calculation.
        error_col (str, optional): Column to calculate total error. Defaults to "Error".

    Returns:
        float: Total error for the data.
    """
    return df.select(pl.col(error_col).sum().alias("Total Error")).collect().item()


def _calc_prorrata(
    df: pl.LazyFrame,
    target_col: str = "Prorrata",
    error_col: str = "Error",
    weight_col: str = "Max Capacity",
    over_col: str = "datetime",
) -> pl.LazyFrame:
    """Redistributes the generation prorate over the hours based on the curtailment and the weight of the generator max capacity.
    This can be set for diferent columns and over different time frames.

    Args:
        df (pl.LazyFrame): Original data for prorate calculation.
        target_col (str, optional): Target column to use for prorate. Defaults to "Prorrata".
        error_col (str, optional): Total energy to use to prorate. Defaults to "Error".
        weight_col (str, optional): Max capacity of the generator. Defaults to "Max Capacity".
        over_col (str, optional): Column for windows function. Defaults to "datetime".

    Returns:
        pl.LazyFrame: Data with new column for prorate - should replace the "generation".
    """
    return df.with_columns(
        (
            pl.col(target_col)
            - pl.col(error_col).sum().over(over_col)
            * pl.col(weight_col)
            / pl.col(weight_col).sum().over(over_col)
        ).alias("Prorrata"),
    )


def _process_prorrata(
    df: pl.LazyFrame,
    target_col: str = "Prorrata",
    error_col: str = "Error",
    weight_col: str = "Max Capacity",
    over_col: str = "datetime",
) -> pl.LazyFrame:
    """Aglomerate the prorate calculation and error checking in a single function. The process is repeated until the error is less than the tolerance.

    Args:
        df (pl.LazyFrame): Orginal data for prorate calculation.
        target_col (str, optional): Target column to use for prorate. Defaults to "Prorrata".
        error_col (str, optional): Total energy to use to prorate. Defaults to "Error".
        weight_col (str, optional): Max capacity of the generator. Defaults to "Max Capacity".
        over_col (str, optional): Column for windows function. Defaults to "datetime".

    Returns:
        pl.LazyFrame: Data with new column for prorate - should replace the "generation".
    """
    df_processed = _calc_prorrata(df, target_col, error_col)
    df_processed = _calc_error(df_processed)

    # TODO: Add logging
    #print(f"error en iteración: {_show_total_error(df_processed)}")

    if _check_error(df_processed):
        return _process_prorrata(df_processed)

    return df_processed


def _pivot_gen(
    df: pl.DataFrame, banned: pl.DataFrame, pmgd: pl.DataFrame
) -> pl.DataFrame:
    """Pivot the generation data to a wide format and filter out the banned generators and the PMGDs.

    Args:
        df (pl.DataFrame): Generation data.
        banned (pl.DataFrame): Banned generators.
        pmgd (pl.DataFrame): PMGDs.

    Returns:
        pl.LazyFrame: Data in wide format with the banned generators and PMGDs filtered out.
    """
    return (
        df.filter(
            ~pl.col("generator").is_in(banned["Centrales"].unique()),
            ~pl.col("generator").is_in(pmgd["Centrales"].unique()),
        )
        .pivot(values="value", columns="property", index=["generator", "datetime"])
        .filter(
            pl.col("Units Generating") == 1,
        )
        .select(pl.exclude("Units Generating"))
    )

