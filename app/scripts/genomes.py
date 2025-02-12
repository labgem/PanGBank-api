import gzip
import logging
from typing import List

import typer
from rich.progress import track
from sqlmodel import Session, select

from app.models import Genome, GenomeSourceInput, GenomeSource


app = typer.Typer(no_args_is_help=True)


def create_genomes(
    genome_sources: list[GenomeSource],
    source_to_genomes: dict[str, list[str]],
    session: Session,
):
    """
    Assign genome sources to genomes and add new genomes to the database if they do not already exist.

    :param genome_sources: A list of genome sources to be processed.
    :param source_to_genomes: A dictionary mapping genome source names to lists of genome names.
    :param session: The database session.
    """

    genome_name_to_genomes: dict[str, Genome] = {}

    for genome_source in genome_sources:

        new_genomes: list[Genome] = []

        name_to_existing_genome = {
            genome.name: genome
            for genome in session.exec(
                select(Genome).where(Genome.genome_source == genome_source)
            ).all()
        }
        logging.info(
            f"Found {len(name_to_existing_genome)} genomes from '{genome_source.name}' in the database."
        )

        for genome_name in track(
            source_to_genomes[genome_source.name],
            description=f"Processing genomes from {genome_source.name}",
        ):
            if genome_name in name_to_existing_genome:
                genome = name_to_existing_genome[genome_name]
            else:
                genome = Genome(name=genome_name)
                new_genomes.append(genome)

            genome_name_to_genomes[genome_name] = genome

        if new_genomes:
            logging.info(f"Adding {len(new_genomes)} new genomes to the database.")
            session.add_all(new_genomes)

            for new_genome in new_genomes:
                new_genome.genome_source = genome_source

        else:
            logging.info(
                "No new genomes to add. All provided genomes are already present in the database."
            )
        session.refresh(genome_source)
        session.commit()

    return genome_name_to_genomes


def parse_genome_to_source_files(
    genome_source_inputs: list[GenomeSourceInput],
) -> dict[str, list[str]]:
    """
    Parse genome-to-source mapping from multiple source files.

    :param source_genomes_files: A list of file paths, each containing genome names associated with a source.
    :return: A dictionary mapping genome names to their respective source names.
    """

    source_to_genomes: dict[str, list[str]] = {}

    for genome_source_input in genome_source_inputs:

        open_func = gzip.open if genome_source_input.file.suffix == ".gz" else open

        with open_func(genome_source_input.file, "rt") as fl:

            source_to_genomes[genome_source_input.name] = [line.strip() for line in fl]

    return source_to_genomes


def create_genome_sources(
    genome_source_inputs: list[GenomeSourceInput], session: Session
):
    """
    Create genome sources from the provided inputs and add them to the database.

    :param genome_source_inputs: A list of genome source inputs.
    :param session: The database session.
    :return: A list of created or existing genome sources.
    """
    genome_sources: List[GenomeSource] = []

    for genome_source_input in genome_source_inputs:
        statement = select(GenomeSource).where(
            GenomeSource.name == genome_source_input.name
        )
        genome_source = session.exec(statement).first()

        if genome_source is None:
            logging.info(f"Creating a new GenomeSource: {genome_source_input.name}")
            genome_source = GenomeSource.model_validate(genome_source_input)
        else:
            logging.info(
                f"GenomeSource '{genome_source_input.name}' already exists in the database"
            )

        session.add(genome_source)

        genome_sources.append(genome_source)

    session.commit()

    for genome_source in genome_sources:
        session.refresh(genome_source)

    return genome_sources


def add_genomes_to_db(
    genome_source_inputs: list[GenomeSourceInput],
    session: Session,
) -> dict[str, Genome]:
    """ """
    logging.info("Starting to process genomes.")

    genome_sources = create_genome_sources(genome_source_inputs, session)

    source_to_genomes = parse_genome_to_source_files(genome_source_inputs)

    logging.info("Parsing genome source files completed.")

    genome_name_to_genomes = create_genomes(
        genome_sources=genome_sources,
        source_to_genomes=source_to_genomes,
        session=session,
    )
    logging.info("Adding genome to db completed.")

    return genome_name_to_genomes
