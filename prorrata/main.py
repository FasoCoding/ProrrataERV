import typer

from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.console import Console
from rich.table import Table
#from rich import print
from typing_extensions import Annotated

from prorrata.extract import DataExtractor
from prorrata.transform import DataProcessor
from prorrata.load import DataLoader

# TODO implementar graficos para la salida.
#from poc_prorrataerv.graph import graph_results

app = typer.Typer()
console = Console()

PATH_ACCDB_INPUT = r"Datos/Model PRGdia_Full_Definitivo Solution/Model PRGdia_Full_Definitivo Solution.accdb"
PATH_ACCDB_OUTPUT = r"Antecedentes/Model PRGdia_Full_Definitivo Solution.accdb"
PATH_PMGD_EXCLUDE =  r"W:/41 Dpto Pronosticos/Vertimiento_ERNC/Lista_PMGDs.xlsx"
PATH_BANNED_GENERATORS = r"R:/Aplicaciones/Prorrateo_Vertimiento/Centrales_Vetadas.xlsx"

def check_path(path_prg: str) -> Path:
    """Check validity of input path

    Args:
        path_prg (str): path to the PRG folder model.

    Returns:
        Path: root path to the daily PRG.
    """
    temp_path = Path(path_prg)
    temp_path = temp_path.parent.parent
    if not temp_path.exists():
        raise ValueError(f"Path: {temp_path.as_posix()} does not exists.")
    elif not temp_path.joinpath(PATH_ACCDB_INPUT).exists():
        raise ValueError(f"Input ACCDB: {temp_path.joinpath(PATH_ACCDB_INPUT).as_posix()} does not exists.")
    elif not temp_path.joinpath(PATH_ACCDB_OUTPUT).exists():
        raise ValueError(f"Output ACCDB: {temp_path.joinpath(PATH_ACCDB_OUTPUT).as_posix()} does not exists.")
    elif not Path(PATH_PMGD_EXCLUDE).exists():
        raise ValueError(f"PMGD exclusion list: {Path(PATH_PMGD_EXCLUDE).as_posix()} does not exists.")
    elif not Path(PATH_BANNED_GENERATORS).exists():
        raise ValueError(f"Banned generators list: {Path(PATH_BANNED_GENERATORS).as_posix()} does not exists.")
    else:
        console.print(f"[bold green]Path to PRG folder[/bold green]: :boom: {temp_path.as_posix()}")
    return temp_path


# entrada de la aplicación.
# revisar documentación de Typer para más información sobre como manejar argumentos y opciones.
@app.command()
def main(path_prg: Annotated[str, typer.Argument(help="Path to the PRG folder")]):
    """Prorate ERV calculation for the PRG model.
    """
    
    path_prg = check_path(path_prg)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        progress.add_task("Extracting data...", total=None)
        data_extractor = DataExtractor(path_prg.joinpath(PATH_ACCDB_INPUT), Path(PATH_PMGD_EXCLUDE), Path(PATH_BANNED_GENERATORS))
        data_extractor.extract_data()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        progress.add_task("Processing data...", total=None)
        data_processor = DataProcessor(data_extractor)
        data_processor.process_prorrata()
        data_processor.get_t_data(data_extractor)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        progress.add_task("Loading data...", total=None)
        data_loader = DataLoader(path_prg.joinpath(PATH_ACCDB_OUTPUT))
        data_loader.load_data(data_processor)

    print("\n") #saltar una linea para que se vea mejor la salida.
    table = Table("fecha-hora", "Error_total", "Total curtailment",title="Resultados prorrata ERV")
    for row in data_processor.show_results().iter_rows():
        table.add_row(row[0].strftime('%Y-%m-%d %H:%M'),format(row[1],".1f"),format(row[2],".1f"))
    console.print(table)

    # TODO: add results with graphs
