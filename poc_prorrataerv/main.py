import typer

from pathlib import Path

from poc_prorrataerv.sql import (
    sql_cmg,
    sql_gen, 
    sql_node
)

from poc_prorrataerv.extract import (
    create_prg_engine,
    get_access_data,
    get_banned_generators,
    get_pmgd,
)

from poc_prorrataerv.transform import (
    process_prorrata,
    pivot_gen,
    show_restuls
)

app = typer.Typer()

#@app.callback()
@app.command()
def main():
    # Inicio de captura de datos en dataframes
    path_prg = Path(r"./data/Model PRGdia_Full_Definitivo Solution.accdb").absolute()

    prg_engine = create_prg_engine(path_prg)

    df_nodes = get_access_data(sql_node, prg_engine)
    df_gen = get_access_data(sql_gen, prg_engine)
    df_cmg = get_access_data(sql_cmg, prg_engine)

    prg_engine.dispose()

    df_pmgd = get_pmgd()
    df_banned_generators = get_banned_generators()

    df_gen_pivot = pivot_gen(df_gen, df_banned_generators, df_pmgd)

    df_ernc = (
        df_cmg
        .join(df_nodes, on="node", how="inner")
        .join(df_gen_pivot, on=["generator","datetime"], how="inner")
    )

    df_ernc_processed = process_prorrata(df_ernc.lazy(),"Available Capacity","Capacity Curtailed")

    print(show_restuls(df_ernc_processed))

#if __name__ == "__main__":
#    main()
#