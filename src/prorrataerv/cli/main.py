import typer
from typing import Annotated
from prorrataerv.cli import config


def main(path: Annotated[str, typer.Argument(help="path to file")]) -> None:
    print(path)


app = typer.Typer(callback=main)
app.add_typer(config.app, name="config")


# @app.command()
# def main(path: Annotated[str, typer.Argument(help="path to file")]) -> None:
#     print(path)
