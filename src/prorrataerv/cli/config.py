import typer

app: typer.Typer = typer.Typer()


@app.callback()
def main() -> None:
    """
    Agrupación de acciones para chequear bids.
    """
