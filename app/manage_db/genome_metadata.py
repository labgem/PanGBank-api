from sqlmodel import Session, select
import logging

from pathlib import Path
from typing import Generator

from app.models import Genome, GenomeMetadata, GenomeMetadataBase, MetadataType

import csv
import gzip
import typer

from app.database import create_db_and_tables, engine

from typing_extensions import Annotated
from rich.progress import track

app = typer.Typer(no_args_is_help=True)


def guess_type(value: str):
    """Try to convert a string value to int, float, or bool, otherwise return as string."""
    if value.lower() in {"true", "false", "f", "t", "1", "0", "yes", "no"}:
        return "bool"
    if value.isdigit():
        return "int"  # Convert numeric strings to int
    try:
        float(value)  # Convert decimal numbers to float
        return "float"
    except ValueError:
        return "str"  # Return original string if conversion fails


def parse_metadata_table(
    file_path: Path,
) -> Generator[tuple[str, list[GenomeMetadataBase]], None, None]:
    """Parse a gzip-compressed TSV and yield rows as dictionaries."""
    proper_open = gzip.open if file_path.name.endswith("gz") else open
    with proper_open(file_path, mode="rt") as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter="\t")
        for row in reader:
            try:
                genome_name = row["Genome"]
            except KeyError:
                raise KeyError(
                    f"Missing 'Genome' column in {file_path}. "
                    "Ensure the TSV contains a 'Genome' column."
                )
            genome_metadata_list = [
                GenomeMetadataBase(
                    key=key,
                    value=str(value),
                    type=MetadataType(guess_type(value)),
                )
                for key, value in row.items()
                if key != "Genome"
            ]

            yield genome_name, genome_metadata_list


def add_metadata(
    genome_name: str, metadata_list: list[GenomeMetadataBase], session: Session
):
    """ """

    genome = session.exec(select(Genome).where(Genome.name == genome_name)).first()

    if genome is None:
        logging.debug(
            f"There are no genome named {genome_name} in the database. Cannot add metadata to it."
        )

    else:
        logging.info(f"Adding metadata to genome {genome_name}.")
        for metadata_input in metadata_list:
            metadata = GenomeMetadata.model_validate(
                metadata_input, update={"genome": genome}
            )
            session.add(metadata)


@app.command(no_args_is_help=True)
def add(
    metadata_file: Annotated[
        Path,
        typer.Argument(
            help="TSV file containing genome metadata, including a 'Genome' column for mapping genomes."
        ),
    ],
):

    create_db_and_tables()

    with Session(engine) as session:

        for genome_name, genome_metadata in track(
            parse_metadata_table(metadata_file),
            description="Processing genome metadata",
        ):

            add_metadata(genome_name, genome_metadata, session=session)

    session.commit()
