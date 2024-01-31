import typer
import io

from typing_extensions import Annotated

from poc_prorrataerv.extract import DataExtractor
from poc_prorrataerv.transform import DataProcessor
from poc_prorrataerv.load import DataLoader

app = typer.Typer()


@app.command()
def main(path_prg: Annotated[str, typer.Argument(help="Path to the PRG folder")]):

    data_extractor = DataExtractor(path_prg)
    data_extractor.extract_data()

    data_processor = DataProcessor(data_extractor)
    data_processor.process_prorrata()
    data_processor.get_t_data(data_extractor)

    # TODO write to csv to pipe on thermngraph or use in here.
    # se puede agregar esto al final: .write_csv(include_header=False))
    print(data_processor.show_restuls())
    data_processor.t_data_0.collect().write_csv("t_data_0.csv")

    data_loader = DataLoader(path_prg)
    data_loader.load_data(data_processor)

    print("Done!")
