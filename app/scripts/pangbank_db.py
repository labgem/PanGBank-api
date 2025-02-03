from pathlib import Path
from typing import Optional
import logging

from sqlmodel import Session, select
import typer
from rich.progress import track
from rich.console import Console
from rich.table import Table
from app.database import create_db_and_tables, engine

from app.scripts.collection import (
    add_collection_release,
    delete_collection_release,
    delete_collection as rm_collection,
)
from app.scripts.genome_metadata import parse_metadata_table, add_metadata

from app.models import Collection, CollectionRelease

cli = typer.Typer()


@cli.command()
def add_collection(collection_dir: Path):

    create_db_and_tables()

    # Define file paths
    collection_release_info_file = collection_dir / "collection_release_info.json"
    taxonomy_source_info_file = collection_dir / "taxonomy_release_info.json"
    genome_sources_info_file = collection_dir / "genome_source_info.json"
    genome_source_dir = collection_dir / "genome_sources"
    pangenome_dir = collection_dir / "pangenomes"
    taxonomy_file = collection_dir / "taxonomy.tsv.gz"

    with Session(engine) as session:
        add_collection_release(
            session,
            collection_release_info_file,
            taxonomy_source_info_file,
            genome_sources_info_file,
            genome_source_dir,
            pangenome_dir,
            taxonomy_file,
        )


@cli.command()
def delete_collection(collection_name: str, release_version: Optional[str] = None):

    create_db_and_tables()

    with Session(engine) as session:

        if release_version:
            delete_collection_release(session, collection_name, release_version)

        else:
            rm_collection(session, collection_name)


@cli.command()
def list_collection():

    create_db_and_tables()

    console = Console()
    with Session(engine) as session:
        statement = select(Collection)

        results = session.exec(statement).all()

        if not results:
            console.print("[bold red]No collections found in the database.[/bold red]")
            return

        for collection in results:
            table = Table(
                title=f"Collections {collection.name}",
                caption=collection.description,
                show_header=True,
                header_style="bold magenta",
            )
            table.add_column("Release", style="bold cyan")
            table.add_column("Pangenomes", justify="right")
            table.add_column("Note", style="dim")

            for collection_release in collection.collection_releases:
                table.add_row(
                    str(collection_release.version),
                    str(len(collection_release.pangenomes)),
                    collection_release.release_note,
                )

        console.print(table)


@cli.command()
def add_genome_metadata(metadata_file: Path):
    create_db_and_tables()

    with Session(engine) as session:

        for genome_metadata in track(
            parse_metadata_table(metadata_file),
            description="Processing genome metadata",
        ):

            try:
                genome_name = genome_metadata.pop("Genome")
            except KeyError:
                raise KeyError(
                    f"Missing 'Genome' column in {metadata_file}. "
                    "Ensure the TSV contains a 'Genome' column."
                )
            add_metadata(genome_name, genome_metadata, session=session)

    session.commit()


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    cli()
