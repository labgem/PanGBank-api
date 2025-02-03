from operator import and_
from sqlmodel import Session, select
from pathlib import Path
import json
import csv
import logging

# import sys
import yaml

from typing import Iterator
import gzip

from rich.progress import track

# # Add the project root to the sys.path
# sys.path = [str(Path(__file__).resolve().parent.parent)] + sys.path

from app.models import (
    Collection,
    CollectionRelease,
    Genome,
    Pangenome,
    GenomePangenomeLink,
    GenomeSource,
    PangenomeMetric,
    GenomeInPangenomeMetric,
)

from .taxonomy import (
    create_taxonomy_source,
    parse_taxonomy_file,
    manage_genome_taxonomies,
    build_taxon_dict,
    parse_ranks_str,
)


def add_source_to_genomes(
    pangenome: Pangenome,
    genome_sources: list[GenomeSource],
    genome_to_source: dict[str, str],
    session: Session,
) -> None:
    """
    Assign genome sources to genomes within a given pangenome.

    :param pangenome: The pangenome containing genome links.
    :param genome_sources: A list of available genome sources.
    :param genome_to_source: A mapping of genome names to their respective source names.
    :param session: SQLAlchemy session for database transactions.
    :return: None
    """

    # Build a lookup dictionary for genome sources by name
    source_name_to_genome_source = {
        genome_source.name: genome_source for genome_source in genome_sources
    }

    # Assign genome sources to genomes linked to the pangenome
    for genome_link in pangenome.genome_links:
        source_name = genome_to_source.get(genome_link.genome.name)

        if source_name is None:
            logging.warning(
                f"No source found for genome {genome_link.genome.name}, skipping assignment."
            )
            continue

        genome_source = source_name_to_genome_source.get(source_name)

        if genome_source is None:
            logging.warning(
                f"Genome source '{source_name}' not found for genome {genome_link.genome.name}."
            )
            continue

        genome_link.genome.genome_source = genome_source

    # Commit all changes to the database
    session.add_all(genome_sources)
    session.commit()

    logging.info(
        f"Successfully assigned genome sources to genomes in pangenome {pangenome.file_name}."
    )


def parse_genome_to_source_files(source_genomes_files: list[Path]) -> dict[str, str]:
    """
    Parse genome-to-source mapping from multiple source files.

    :param source_genomes_files: A list of file paths, each containing genome names associated with a source.
    :return: A dictionary mapping genome names to their respective source names.
    """

    genome_to_source = {}

    for genome_to_source_file in source_genomes_files:

        with open(genome_to_source_file) as fl:
            source = genome_to_source_file.stem
            genome_to_source.update({line.strip(): source for line in fl})

    return genome_to_source


def create_genome_sources(
    genome_sources_info_file: Path, session: Session
) -> list[GenomeSource]:
    """
    Create and retrieve genome sources from the database.

    :param genome_sources_info_file: Path to the JSON file containing genome source information.
    :param session: SQLAlchemy session for database transactions.
    :return: A list of GenomeSource objects.
    """
    genome_sources = []

    with open(genome_sources_info_file) as fl:
        genome_sources_info = json.load(fl)

    for genome_info in genome_sources_info:
        # Check if genome source already exists in DB
        statement = select(GenomeSource).where(GenomeSource.name == genome_info["name"])
        genome_source = session.exec(statement).first()

        if genome_source is None:
            logging.info("Creating a new GenomeSource")
            genome_source = GenomeSource(**genome_info)
        else:
            logging.info("GenomeSource already exists in DB")

        session.add(genome_source)
        genome_sources.append(genome_source)

    session.commit()

    for genome_source in genome_sources:
        session.refresh(genome_source)

    return genome_sources


def create_collection_release(
    collection_release_info_file: Path, session: Session
) -> CollectionRelease:
    """
    Create or retrieve a collection release from the database.

    :param collection_release_info_file: Path to the JSON file containing collection release details.
    :param session: SQLModel session for database transactions.
    :return: The corresponding CollectionRelease object.

    This function:
    - Reads collection and release information from the provided JSON file.
    - Ensures the collection exists in the database, creating it if necessary.
    - Ensures the collection release exists in the database, creating it if necessary.
    - Validates version consistency between the input file and existing database records.
    """

    with open(collection_release_info_file) as fl:
        collection_release_info = json.load(fl)

    collection = Collection.model_validate(
        collection_release_info.get("collection", {})
    )
    print(collection)

    statement = select(Collection).where((Collection.name == collection.name))

    collection_from_db = session.exec(statement).first()

    if collection_from_db is None:
        logging.info(f"Adding a new collection to DB: {collection.name}")
        session.add(collection)
        session.commit()
    else:
        collection = collection_from_db
        logging.info(f"Collection {collection.name} already exists in DB")

    collection_release = CollectionRelease.model_validate(
        collection_release_info.get("release", {})
    )

    statement = (
        select(CollectionRelease)
        .join(Collection)
        .where(
            (Collection.name == collection.name)
            & (CollectionRelease.version == collection_release.version)
        )
    )

    collection_release_from_db = session.exec(statement).first()

    if collection_release_from_db is None:

        logging.info(
            f"Adding a new collection release to DB: {collection.name}:{collection_release.version}"
        )
        collection_release.collection = collection

        session.add(collection_release)
        session.commit()

    else:
        logging.info(
            f"Collection release {collection.name}:{collection_release.version} already exists in DB"
        )

        same_ppanggo_version = (
            collection_release.ppanggolin_version
            == collection_release_from_db.ppanggolin_version
        )
        same_pangbank_wf_version = (
            collection_release.pangbank_wf_version
            == collection_release_from_db.pangbank_wf_version
        )

        if not same_ppanggo_version or not same_pangbank_wf_version:
            raise ValueError(
                f"For collection {collection.name} release {collection_release.version}:"
                "Not the same ppanggolin_version or pangbank_wf_version from input file and whats in the DB.. "
                f"ppanggolin version : {collection_release.ppanggolin_version} vs {collection_release.ppanggolin_version} "
                f"ppanggolin version : {collection_release.pangbank_wf_version} vs {collection_release.pangbank_wf_version} "
            )

        collection_release = collection_release_from_db

    session.commit()
    session.refresh(collection)

    return collection_release


def parse_genomes_hash_file(genomes_md5sum_file: Path) -> dict[str, dict[str, str]]:
    """
    Parse a genome hash file to extract genome name-to-metadata mapping.

    :param genomes_md5sum_file: Path to the TSV file containing genome MD5 checksums.
    :return: A dictionary mapping genome names to their metadata, extracted from the file.

    The input file is expected to be a tab-separated values (TSV) file with at least
    a 'name' column for genome identifiers.
    """

    if not genomes_md5sum_file.exists():
        raise FileNotFoundError(
            f"Genome MD5 checksum file not found: {genomes_md5sum_file}"
        )

    with open(genomes_md5sum_file) as fl:
        genome_name_to_genome_info = {
            genome_info["name"]: genome_info
            for genome_info in csv.DictReader(fl, delimiter="\t")
        }

    return genome_name_to_genome_info


def parse_genome_metrics_file(tsv_file_path: Path) -> Iterator[GenomeInPangenomeMetric]:
    """
    Parse a TSV file containing genome data and yield GenomeInPangenomeMetric instances.

    :param tsv_file_path: Path to the TSV (or TSV.GZ) file containing genome metrics.
    :return: An iterator of GenomeInPangenomeMetric instances.

    The function:
    - Supports both uncompressed (.tsv) and gzip-compressed (.tsv.gz) files.
    - Skips comment lines (lines starting with `#`).
    - Converts column headers to lowercase before validation.
    - Raises a ValueError if a row fails to validate.
    """

    if not tsv_file_path.exists():
        raise FileNotFoundError(f"Genome metrics file not found: {tsv_file_path}")

    open_func = gzip.open if tsv_file_path.suffix == ".gz" else open

    with open_func(tsv_file_path, mode="rt", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(filter(lambda row: row[0] != "#", file), delimiter="\t")

        for row in reader:
            try:
                genome_data = GenomeInPangenomeMetric.model_validate(
                    {key.lower(): value for key, value in row.items()}
                )
                yield genome_data

            except ValueError as e:
                raise ValueError(f"Error parsing row {row}: {e}") from e


def get_pangenome_metrics_from_info_file(yaml_file_path: Path) -> PangenomeMetric:
    with open(yaml_file_path, "r") as file:
        data = yaml.safe_load(file)

    # Initialize pangenome_data
    pangenome_data = {
        "gene_count": data.get("Content", {}).get("Genes"),
        "genome_count": data.get("Content", {}).get("Genomes"),
        "family_count": data.get("Content", {}).get("Families"),
        "edge_count": data.get("Content", {}).get("Edges"),
        # Other information
        "partition_count": data.get("Content", {}).get("Number_of_partitions"),
        "rgp_count": data.get("Content", {}).get("RGP"),
        "spot_count": data.get("Content", {}).get("Spots"),
        # Modules information
        "module_count": data.get("Content", {})
        .get("Modules", {})
        .get("Number_of_modules"),
        "family_in_module_count": data.get("Content", {})
        .get("Modules", {})
        .get("Families_in_Modules"),
    }

    # Loop through partitions to populate family counts and genome frequency metrics
    for partition in ["Persistent", "Shell", "Cloud"]:
        partition_key = partition.lower()

        pangenome_data[f"{partition_key}_family_count"] = (
            data.get("Content", {}).get(partition, {}).get("Family_count")
        )
        pangenome_data[f"{partition_key}_family_min_genome_frequency"] = (
            data.get("Content", {}).get(partition, {}).get("min_genomes_frequency")
        )
        pangenome_data[f"{partition_key}_family_max_genome_frequency"] = (
            data.get("Content", {}).get(partition, {}).get("max_genomes_frequency")
        )
        pangenome_data[f"{partition_key}_family_std_genome_frequency"] = (
            data.get("Content", {}).get(partition, {}).get("sd_genomes_frequency")
        )
        pangenome_data[f"{partition_key}_family_mean_genome_frequency"] = (
            data.get("Content", {}).get(partition, {}).get("mean_genomes_frequency")
        )

    return PangenomeMetric.model_validate(pangenome_data)


def parse_pangenome_dir(
    pangenome_main_dir: Path, collection_release: CollectionRelease, session: Session
) -> list[Pangenome]:
    """
    Parse the pangenome directory and load pangenome data into the database.

    :param pangenome_main_dir: The main directory containing pangenome subdirectories.
    :param collection_release: The collection release to associate the pangenomes with.
    :param session: SQLAlchemy session for database transactions.
    :return: A list of Pangenome objects.
    """
    pangenomes = []

    for pangenome_dir in pangenome_main_dir.iterdir():
        if not pangenome_dir.is_dir():
            logging.info(f"Skipping {pangenome_dir}")
            continue

        pangenome_file = pangenome_dir / "pangenome.h5"
        genomes_md5sum_file = pangenome_dir / "genomes_md5sum.tsv"
        pangenome_info_file = pangenome_dir / "info.yaml"
        genomes_statistics_file = pangenome_dir / "genomes_statistics.tsv.gz"

        pangenome_local_path = Path(pangenome_file.parent.name) / pangenome_file.name

        pangenome = session.exec(
            select(Pangenome).where(
                and_(
                    Pangenome.file_name == pangenome_local_path.as_posix(),
                    Pangenome.collection_release == collection_release,
                )
            )
        ).first()

        if pangenome is None:
            pangenome_specific_args = {
                "file_name": pangenome_local_path.as_posix(),
                "collection_release": collection_release,
            }
            pangenome_metric = get_pangenome_metrics_from_info_file(pangenome_info_file)
            pangenome = Pangenome.model_validate(
                pangenome_metric, from_attributes=True, update=pangenome_specific_args
            )
            session.add(pangenome)

        # Get genomes that belong to pangenome and associate them to it
        genome_name_to_md5sum_info = parse_genomes_hash_file(genomes_md5sum_file)

        for genome_metric in parse_genome_metrics_file(genomes_statistics_file):
            genome = session.exec(
                select(Genome).where(Genome.name == genome_metric.genome_name)
            ).first()

            if genome is None:
                genome = Genome(
                    name=genome_metric.genome_name
                )  # Add genome version if given?
                session.add(genome)

            genome_pangenome_link = session.exec(
                select(GenomePangenomeLink).where(
                    and_(
                        GenomePangenomeLink.genome == genome,
                        GenomePangenomeLink.pangenome == pangenome,
                    )
                )
            ).first()

            if genome_pangenome_link is None:
                genome_file_info = genome_name_to_md5sum_info[genome_metric.genome_name]

                pangenome_genome_link = GenomePangenomeLink.model_validate(
                    genome_metric,
                    from_attributes=True,
                    update={
                        "genome": genome,
                        "pangenome": pangenome,
                        "genome_file_md5sum": genome_file_info["md5_sum"],
                        "genome_file_name": genome_file_info["file_name"],
                    },
                )
                session.add(pangenome_genome_link)
                pangenome.genome_links.append(pangenome_genome_link)

        pangenomes.append(pangenome)

    session.commit()
    session.refresh(collection_release)

    return pangenomes


def add_collection_release(
    session: Session,
    collection_release_info_file: Path,
    taxonomy_source_info_file: Path,
    genome_sources_info_file: Path,
    genome_source_dir: Path,
    pangenome_dir: Path,
    taxonomy_file: Path,
):
    """
    Process and integrate genome, taxonomy, and pangenome data into the database.

    This function:
    - Parses and loads taxonomy, genome sources, and pangenome data.
    - Maps genomes to taxonomy and genome sources.
    - Logs key statistics on collection releases, taxonomy sources, and genome sources.

    """

    genome_to_taxonomy = parse_taxonomy_file(taxonomy_file)
    source_genomes_files = list(genome_source_dir.iterdir())

    collection_release = create_collection_release(
        collection_release_info_file, session=session
    )
    logging.info(
        f"The collection release has {len(collection_release.pangenomes)} pangenomes"
    )

    pangenomes = parse_pangenome_dir(
        pangenome_dir, collection_release=collection_release, session=session
    )
    taxonomy_source = create_taxonomy_source(taxonomy_source_info_file, session=session)
    logging.info(f"The taxonomy source has {len(taxonomy_source.taxa)} taxa")

    existing_taxon_dict = build_taxon_dict(taxonomy_source.taxa)
    ranks = parse_ranks_str(taxonomy_source.ranks)

    # Process genome taxonomies
    for pangenome in track(pangenomes, description="Processing genome taxonomies"):
        manage_genome_taxonomies(
            pangenome=pangenome,
            genome_to_taxonomy=genome_to_taxonomy,
            taxonomy_source=taxonomy_source,
            existing_taxon_dict=existing_taxon_dict,
            ranks=ranks,
            session=session,
        )

    genome_sources = create_genome_sources(genome_sources_info_file, session=session)
    genome_to_source = parse_genome_to_source_files(source_genomes_files)

    # Assign genome sources to genomes
    for pangenome in pangenomes:
        add_source_to_genomes(pangenome, genome_sources, genome_to_source, session)

    logging.info(
        f"The taxonomy source {taxonomy_source.name}-{taxonomy_source.version} has {len(taxonomy_source.taxa)} taxa"
    )
    logging.info(
        f"The collection release {collection_release.collection.name}-{collection_release.version} has {len(collection_release.pangenomes)} pangenomes"
    )

    for genome_source in genome_sources:
        logging.info(
            f"The genome source {genome_source.name} has {len(genome_source.genomes)} genomes"
        )


def delete_collection(session: Session, collection_name: str) -> None:
    """
    Deletes a collection from the database if it exists.

    :param session: Database session used for querying and deleting the collection.
    :param collection_name: Name of the collection to delete.
    """
    # Query the database to find the collection with the specified name
    statement = select(Collection).where(Collection.name == collection_name)
    collection_from_db = session.exec(statement).first()

    if collection_from_db is None:
        logging.info(
            f"Collection '{collection_name}' not found in the database. Deletion aborted."
        )
    else:
        logging.info(f"Deleting collection '{collection_name}' from the database.")
        session.delete(collection_from_db)
        session.commit()


def delete_collection_release(
    session: Session, collection_name: str, release_version: str
) -> None:
    """
    Deletes a specific collection release from the database if it exists.

    :param session: Database session used for querying and deleting the collection release.
    :param collection_name: Name of the collection containing the release.
    :param release_version: Version of the collection release to delete.
    """
    # Query the database to find the collection release with the specified name and version
    statement = (
        select(CollectionRelease)
        .join(Collection)
        .where(
            (Collection.name == collection_name)
            & (CollectionRelease.version == release_version)
        )
    )
    collection_release_from_db = session.exec(statement).first()

    if collection_release_from_db is None:
        logging.info(
            f"Collection release '{collection_name}' (version: {release_version}) not found in the database. Deletion aborted."
        )
    else:
        logging.info(
            f"Deleting collection release '{collection_name}' (version: {release_version}) from the database."
        )
        session.delete(collection_release_from_db)
        session.commit()
