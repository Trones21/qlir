import click

from qlir.data.sources.base import DataSource


@click.group()
def instruments():
    """Instrument-related commands."""
    pass


@instruments.command()
@click.option("--source", type=click.Choice([d.value for d in DataSource]), required=True)
def discover(source):
    """Discover markets available at a given datasource."""
    raise NotImplementedError(
        f"Market discovery not implemented yet for source={source}"
    )
