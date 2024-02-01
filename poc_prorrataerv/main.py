import typer
from datetime import datetime

from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.console import Console
from rich.table import Table
from typing_extensions import Annotated

from poc_prorrataerv.extract import DataExtractor
from poc_prorrataerv.transform import DataProcessor
from poc_prorrataerv.load import DataLoader
#from poc_prorrataerv.graph import graph_results

app = typer.Typer()
console = Console()


@app.command()
def main(path_prg: Annotated[str, typer.Argument(help="Path to the PRG folder")]):
    """Prorate ERV calculation for the PRG model.
    """

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        progress.add_task("Extracting data...", total=None)
        data_extractor = DataExtractor(path_prg)
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
        data_loader = DataLoader(path_prg)
        data_loader.load_data(data_processor)

    table = Table("fecha-hora", "Generación", "Prorrata",title="Resultados prorrata ERV")
    for row in data_processor.show_results().iter_rows():
        table.add_row(row[0].strftime('%Y-%m-%d %H:%M'),format(row[1],".2f"),format(row[2],".2f"))
    console.print(table)

    # TODO: add results with graphs
