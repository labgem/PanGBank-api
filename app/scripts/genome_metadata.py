from sqlmodel import Session, select
import logging
import typer

from pathlib import Path
from typing import Dict

from app.models import Genome, GenomeMetadata

import csv
import gzip


def try_convert(value: str):
    """Try to convert a string value to int, float, or bool, otherwise return as string."""
    if value.lower() in {"true", "false", "f", "t", "1", "0", "yes", "no"}:
        return value.lower() in [
            "true",
            "t",
            "1",
            "yes",
        ]  # Convert "true"/"false" to boolean
    if value.isdigit():
        return int(value)  # Convert numeric strings to int
    try:
        return float(value)  # Convert decimal numbers to float
    except ValueError:
        return value  # Return original string if conversion fails


def parse_metadata_table(file_path: Path):
    """Parse a gzip-compressed TSV and yield rows as dictionaries."""
    proper_open = gzip.open if file_path.name.endswith("gz") else open
    with proper_open(file_path, mode="rt") as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter="\t")
        for row in reader:

            parsed_row = {key: try_convert(value) for key, value in row.items()}
            yield dict(parsed_row)


def add_metadata(genome_name: str, metadata: Dict[str, str], session: Session):
    """ """

    genome = session.exec(select(Genome).where(Genome.name == genome_name)).first()

    if genome is None:
        logging.debug(
            f"There are no genome named {genome_name} in the database. Cannot add metadata to it."
        )

    else:
        logging.info(f"Adding metadata to genome {genome_name}.")
        for key, value in metadata.items():
            metadata = GenomeMetadata(
                key=key,
                value=str(value),
                type=type(value).__name__,
                genome_id=genome.id,
                genome=genome,
            )
            session.add(metadata)
