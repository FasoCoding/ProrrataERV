import typer

from pcp_check.presentation.cli import bids, sscc

app = typer.Typer(pretty_exceptions_show_locals=False)
app.add_typer(bids.app, name="bids")
app.add_typer(sscc.app, name="sscc")
