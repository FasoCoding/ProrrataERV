import typer

from rich.progress import Progress, SpinnerColumn, TextColumn
from typing_extensions import Annotated

from poc_prorrataerv.extract import DataExtractor
from poc_prorrataerv.transform import DataProcessor
from poc_prorrataerv.load import DataLoader

app = typer.Typer()


@app.command()
def main(path_prg: Annotated[str, typer.Argument(help="Path to the PRG folder")]):

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

    print("Done!")
    # TODO: add results with graphs and tables.
    print(data_processor.show_restuls())
