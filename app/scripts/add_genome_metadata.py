from sqlmodel import Session, select
import logging
import typer

from pathlib import Path
from typing import Dict

from ..database import create_db_and_tables, engine
from ..models import Genome, GenomeMetadata

import csv
import gzip


from rich.progress import track


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


def add_genome_metadata(genome_name: str, metadata: Dict[str, str], session: Session):
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


def main(metadata_file: Path):
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

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
            add_genome_metadata(genome_name, genome_metadata, session=session)
        session.commit()


if __name__ == "__main__":
    typer.run(main)
