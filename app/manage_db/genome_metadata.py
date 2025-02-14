from sqlmodel import Session, select
import logging

from pathlib import Path
from typing import Generator, Optional

from app.models import (
    Genome,
    GenomeMetadata,
    GenomeMetadataBase,
    GenomePangenomeLink,
    GenomeMetadataSource,
    GenomeSource,
)
from app.manage_db.utils import set_up_logging_config, check_and_read_json_file
import csv
import gzip
import typer

from app.database import create_db_and_tables, engine

from typing_extensions import Annotated
from rich.progress import track

app = typer.Typer(no_args_is_help=True)


def parse_metadata_table(
    file_path: Path,
) -> Generator[tuple[str, list[GenomeMetadataBase]], None, None]:
    """Parse a gzip-compressed TSV and yield rows as dictionaries."""
    proper_open = gzip.open if file_path.name.endswith("gz") else open
    with proper_open(file_path, mode="rt") as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter="\t")
        for row in track(reader, "Parsing metadata", total=None):
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
                )
                for key, value in row.items()
            ]

            yield genome_name, genome_metadata_list


def create_metadata(
    genome_id: int,
    metadata_list: list[GenomeMetadataBase],
    source: GenomeMetadataSource,
):
    """ """
    metadatas: list[GenomeMetadata] = []

    for metadata_input in metadata_list:
        metadata = GenomeMetadata.model_validate(
            metadata_input, update={"genome_id": genome_id, "source_id": source.id}
        )
        metadatas.append(metadata)

    return metadatas


def parse_metadata_source_file(metadata_source_file: Path):

    json_content = check_and_read_json_file(metadata_source_file)

    metadata_source = GenomeMetadataSource.model_validate(json_content)

    return metadata_source


def get_all_genomes_in_pangenome(session: Session):

    genomes_statement = (
        select(Genome)
        .join(GenomePangenomeLink)
        .where(Genome.id == GenomePangenomeLink.genome_id)
    )
    genomes_with_links = session.exec(genomes_statement).all()

    return genomes_with_links


def get_all_genomes_from_genome_source(session: Session, genome_source: str):
    # select all genome that are linked with a pangenome in the database

    genomes_statement = session.exec(
        select(Genome).join(GenomeSource).where(GenomeSource.name == genome_source)
    )

    return genomes_statement.all()


def add_genome_source_to_db(metadata_source: GenomeMetadataSource, session: Session):
    metadata_source_in_db = session.exec(
        select(GenomeMetadataSource).where(
            (GenomeMetadataSource.name == metadata_source.name)
            & (GenomeMetadataSource.version == metadata_source.version)
        )
    ).first()

    if metadata_source_in_db is not None:
        raise ValueError(
            f"Genome Metadata Source '{metadata_source.name}' version: '{metadata_source.version}' already exists in the database."
        )

    session.add(metadata_source)
    session.commit()


@app.command(no_args_is_help=True)
def add(
    metadata_source_file: Annotated[
        Path,
        typer.Argument(
            help="Path to a JSON file that describes the metadata source"
            " contains file information with genome metadata. "
        ),
    ],
    metadata_file: Annotated[
        Path,
        typer.Argument(
            help="Path to a TSV file that contains genome metadata. A column 'Genome' must be present."
        ),
    ],
):

    set_up_logging_config()

    metadata_source = parse_metadata_source_file(metadata_source_file)

    if not metadata_file.exists():
        typer.echo(
            f"[bold red]Error:[/bold red] The file {metadata_file} does not exist",
            err=True,
        )
        raise typer.Exit(1)

    create_db_and_tables()

    with Session(engine) as session:

        add_genome_source_to_db(metadata_source, session)

        # we could add more filter to add metadata to only genomes of interest
        genomes = get_all_genomes_in_pangenome(session)

        genome_name_to_genome_id = {genome.name: genome.id for genome in genomes}

        logging.info(
            f"Retrieved {len(genome_name_to_genome_id)} genomes from the database."
        )

        metadada_list: list[GenomeMetadata] = []

        unknown_genome_count = 0
        genome_processed_count = 0
        logging.info(f"Parsing genome metadata from {metadata_file}.")

        genome_to_metadata = list(parse_metadata_table(metadata_file))

        logging.info(
            f"Metadata for {len(genome_to_metadata)} genomes have been collected."
        )
        for genome_name, genome_metadata in track(
            genome_to_metadata,
            description="Processing metadata",
            total=len(genome_to_metadata),
        ):

            genome_id = genome_name_to_genome_id.get(genome_name)

            if genome_id is not None:
                metadada_list += create_metadata(
                    genome_id, genome_metadata, metadata_source
                )
                genome_processed_count += 1
            else:
                unknown_genome_count += 1

            if genome_processed_count == 5000:
                logging.info(
                    f"Adding new {len(metadada_list)} metadata to the database describing {genome_processed_count} genomes."
                )

                session.add_all(metadada_list)
                genome_processed_count = 0
                metadada_list: list[GenomeMetadata] = []

        logging.info(
            f"Adding final {len(metadada_list)} metadata to the database describing {genome_processed_count} genomes."
        )
        session.add_all(metadada_list)

        if unknown_genome_count > 0:
            logging.info(
                f"{unknown_genome_count} genomes in the metadata file are not in the database."
            )

        session.commit()


@app.command(no_args_is_help=True)
def delete(
    metadata_source_name: Annotated[
        str,
        typer.Argument(help="Name of the metadata source to delete from the database."),
    ],
    metadata_source_version: Annotated[
        Optional[str],
        typer.Option(
            help="Specific metadata source version to delete. If not provided, the entire source will be deleted."
        ),
    ] = None,
):
    """
    Deletes a collection from the database if it exists.

    :param session: Database session used for querying and deleting the collection.
    :param collection_name: Name of the collection to delete.
    """

    set_up_logging_config()

    create_db_and_tables()

    with Session(engine) as session:

        if metadata_source_version is None:
            # Query the database to find the collection with the specified name
            statement = select(GenomeMetadataSource).where(
                GenomeMetadataSource.name == metadata_source_name
            )

        else:
            statement = select(GenomeMetadataSource).where(
                (GenomeMetadataSource.name == metadata_source_name)
                & (GenomeMetadataSource.version == metadata_source_version)
            )

        metadata_sources = session.exec(statement).all()

        if not metadata_sources:
            source_info = (
                f"{metadata_source_name} version: {metadata_source_version}"
                if metadata_source_version
                else metadata_source_name
            )
            logging.info(
                f"Genome Metadata Source '{source_info}' not found in the database. Deletion aborted."
            )
            metadata_sources = session.exec(select(GenomeMetadataSource)).all()
            logging.info(
                f"Available genome metadata sources in the database: {[f"name='{source.name} version='{source.version}'" for source in metadata_sources]}"
            )

        else:
            for source in metadata_sources:
                logging.info(
                    f"Deleting genome metadata source: '{source.name} version={source.version}' from the database."
                )
                session.delete(source)

            session.commit()
