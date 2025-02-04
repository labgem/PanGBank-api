import typer

from app.scripts.collections import app as collection_app
from app.scripts.genome_metadata import app as genome_metadata_app

cli = typer.Typer(no_args_is_help=True)


cli.add_typer(collection_app, name="collections")

cli.add_typer(genome_metadata_app, name="genome_metadata")


if __name__ == "__main__":

    cli()
