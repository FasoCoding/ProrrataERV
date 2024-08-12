import typer

from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.console import Console
from rich.table import Table
from typing_extensions import Annotated

from prorrata.business.extract import process_inputs, check_inputs_health
from prorrata.business.transform import process_prorrata, t_data_update, show_results
from prorrata.business.load import load_data

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
        #TaskProgressColumn(),
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        
        task_extract = progress.add_task("Extracting data...", total=None)
        data, t_data_0, mt = process_inputs(path_prg)
        #data_extractor.extract_data()
        progress.print("listo! datos extraidos :smiley: ...", style="bold cyan")
        progress.remove_task(task_extract)
        
        if  check_inputs_health(data):
            task_data = progress.add_task("Processing data...", total=None)
            #data_processor = DataProcessor(data_extractor)
            data_prorrata = process_prorrata(data)
            t_data_new = t_data_update(data_prorrata, t_data_0, mt)
            progress.print("listo! datos procesados :smiley: ...", style="bold cyan")
            progress.remove_task(task_data)

            task_load = progress.add_task("Loading data...", total=None)
            load_data(path_prg, t_data_new)
            progress.print("listo! datos guardados :smiley: ...", style="bold cyan")
            progress.remove_task(task_load)

            progress.stop()

            print("\n") #saltar una linea para que se vea mejor la salida.
            table = Table("fecha-hora", "Error_total", "Total curtailment",title="Resultados prorrata ERV")
            for row in show_results(data_prorrata).iter_rows():
                table.add_row(row[0].strftime('%Y-%m-%d %H:%M'),format(row[1],".1f"),format(row[2],".1f"))
            if table.row_count > 0:
                console.print(table)

        else:
            progress.print("No hay curtailment! no pasa nada XD... :pile_of_poo:", style="bold red")

    # TODO: add results with graphs
