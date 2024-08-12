import polars as pl

WEIGHT_COL = "Max Capacity"
DATETIME_COL = "datetime"
GENERATION_COL = "Generation"
PRORRATA_COL = "Prorrata"
ERROR_COL = "Error"
AVAILABLE_COL = "Available Capacity"
ACTIVE_COL = "Active"

def show_results(data: pl.DataFrame) -> pl.DataFrame:
    return (
        data
        .group_by("datetime")
        .agg(
            (pl.col("Prorrata").sum() - pl.col("Generation").sum()).alias("Error_Prorrata"),
            pl.col("Capacity Curtailed").sum().alias("Total_Curtailed"),
        )
        .sort(by="datetime")
    )

def t_data_update(data: pl.DataFrame, t_data: pl.DataFrame, mt: pl.DataFrame) -> pl.DataFrame:
    return (
        data.join(
            t_data.filter(pl.col("property") == "Generation"),
            on=["generator", "datetime"],
            how="inner",
        )
        .join(
            mt
            .with_columns(
                pl.datetime(
                    year=pl.col("YEAR"),
                    month=pl.col("MONTH"),
                    day=pl.col("DAY"),
                    hour=pl.col("PERIOD")
                ).alias("datetime")
            )
            .filter(pl.col('NAME').str.contains("_GNL_INF"))
            .select(['NAME','datetime','VALUE'])
            .rename({'NAME': 'generator', 'VALUE': 'MinTech'}),
            on=['generator', 'datetime'],
            how='left'
        )
        .with_columns(
            pl.when(pl.col("MinTech").is_null()).then(pl.col("Prorrata")).otherwise(pl.col("Prorrata")+pl.col("MinTech")).alias("new_value")
        )
        .select(
            pl.col("data_key").alias("key_id"),
            pl.col("data_period").alias("period_id"),
            pl.col("Prorrata").alias("value"),
        )
        .sort(by=["key_id", "period_id"])
    )


def process_prorrata(data: pl.DataFrame) -> pl.DataFrame:
    data_prorrata = _create_prorrata(data)
    data_error = _calc_error(data_prorrata)
    data_end = _process_prorrata(data_error)
    return data_end


def _create_prorrata(data: pl.DataFrame) -> pl.DataFrame:
    return data.with_columns(
        (
            pl.col(GENERATION_COL).sum().over(DATETIME_COL)
            * pl.col(WEIGHT_COL)
            / pl.col(WEIGHT_COL).sum().over(DATETIME_COL)
        ).alias(PRORRATA_COL),
        pl.lit(0).alias(ERROR_COL),
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


def _check_error(df: pl.DataFrame, tol: float = 1e-2) -> bool:
    return df.select(pl.col(ERROR_COL).ge(tol).any()).item()
