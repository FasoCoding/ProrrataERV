import polars as pl

def _calc_error(df: pl.LazyFrame, original_col: str = "Generation", prorrata_col: str = "Prorrata", over_col: str = "datetime") -> pl.LazyFrame:
    return (
        df
        .with_columns(
            pl.when(pl.col("Prorrata").lt(0))
            .then(pl.col("Prorrata").abs())
            .otherwise(0)
            .alias("Error"),
            pl.when(pl.col("Prorrata").lt(0))
            .then(0)
            .otherwise(pl.col("Prorrata"))
            .alias("Prorrata"),
        )
    )

def _check_error(df: pl.LazyFrame, error_col: str = "Error", tol: float = 1e-3) -> bool:
    return df.select(pl.col(error_col).ge(tol).any()).collect().item()

def _show_total_error(df: pl.LazyFrame, error_col: str = "Error") -> float:
    return df.select(pl.col(error_col).sum().alias("Total Error")).collect().item()

def _calc_prorrata(df: pl.LazyFrame, target_col: str = "Prorrata", error_col: str = "Error", weight_col: str = "Max Capacity", over_col: str = "datetime") -> pl.LazyFrame:
    return (
        df
        .with_columns(
            (pl.col(target_col) - pl.col(error_col).sum().over(over_col) * pl.col(weight_col) / pl.col(weight_col).sum().over(over_col)).alias("Prorrata"),
        )
    )

def process_prorrata(df: pl.LazyFrame, target_col: str = "Prorrata", error_col: str = "Error", weight_col: str = "Max Capacity", over_col: str = "datetime") -> pl.LazyFrame:
    df_processed = _calc_prorrata(df,target_col,error_col)
    df_processed = _calc_error(df_processed)

    #print(_check_error(df_processed))
    print(f"error actual: {_show_total_error(df_processed)}")

    if _check_error(df_processed):
        return process_prorrata(df_processed)
    
    return df_processed

def pivot_gen(df: pl.DataFrame, banned: pl.DataFrame, pmgd: pl.DataFrame) -> pl.LazyFrame:
    return (
        df
        .filter(
            ~pl.col("generator").is_in(banned["Centrales"].unique()),
            ~pl.col("generator").is_in(pmgd["Centrales"].unique()),
        )
        .pivot(
            values="value",
            columns="property",
            index=["generator", "datetime"]
        )
        .filter(
            pl.col("Units Generating") == 1,
        )
        .select(
            pl.exclude("Units Generating")
        )
    )

def show_restuls(df: pl.LazyFrame) -> pl.DataFrame:
    return (
        df
        .sort(by="datetime")
        .group_by("datetime")
        .agg(
            pl.col("Generation").sum().alias("Total_Gen"),
            pl.col("Prorrata").sum().alias("Total_Gen_Prorrata"),
            pl.col("Error").sum().alias("Total_Error"),
            (pl.col("Prorrata") - pl.col("Error")).sum().alias("Sum_Prorrata_error"),
            (pl.col("Generation") - (pl.col("Prorrata") - pl.col("Error"))).sum().alias("Test_total"),
        )
        .collect()
    )