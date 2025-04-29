import csv
import gzip
import logging
from pathlib import Path
from typing import Iterator, List
from packaging.version import parse

# import sys
import yaml
from rich.console import Console
from rich.progress import track
from rich.table import Table
from sqlmodel import Session, select

from pangbank_api.database import engine
from pangbank_api.manage_db.taxonomy import get_common_taxa
from pangbank_api.models import (
    Collection,
    CollectionRelease,
    Genome,
    GenomeInPangenomeMetric,
    GenomePangenomeLink,
    Pangenome,
    PangenomeMetric,
    PangenomeTaxonLink,
    Taxon,
    GenomeMetadataSource,
    TaxonomySource,
)

# from pangbank_api.manage_db.genome_metadata import parse_metadata_table
from pangbank_api.manage_db.utils import compute_md5

logger = logging.getLogger(__name__)  # __name__ ensures uniqueness per module


def create_collection_release(
    collection_input: Collection,
    collection_release_input: CollectionRelease,
    taxonomy_source: TaxonomySource,
    genome_metadata_sources: list[GenomeMetadataSource],
    session: Session,
) -> CollectionRelease:
    """
    Create or retrieve a collection release from the database.

    This function:
    - Ensures the collection exists in the database, creating it if necessary.
    - Ensures the collection release exists in the database, creating it if necessary.
    - Validates version consistency between the input file and existing database records.
    """

    collection = Collection.model_validate(collection_input)

    statement = select(Collection).where((Collection.name == collection.name))

    collection_from_db = session.exec(statement).first()

    if collection_from_db is None:
        logger.info(f"Adding a new collection to DB: {collection.name}")
        session.add(collection)
        session.commit()
    else:
        collection = collection_from_db
        logger.info(f"Collection {collection.name} already exists in DB")

    collection_release = CollectionRelease.model_validate(collection_release_input)

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
        logger.info(
            f"Adding a new collection release to DB: {collection.name}:{collection_release.version}"
        )
        collection_release.collection = collection
        collection_release.taxonomy_source = taxonomy_source
        collection_release.genome_metadata_sources = genome_metadata_sources
        logger.info(f"New release is linked to taxonomy source {taxonomy_source.name}")
        logger.info(
            f"New release is linked to genome metadata sources {[source.name for source in genome_metadata_sources]}"
        )

        session.add(collection_release)
        session.commit()

    else:
        logger.info(
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

        same_genome_metadata_sources = {
            source.name for source in genome_metadata_sources
        } == {
            source.name for source in collection_release_from_db.genome_metadata_sources
        }

        if (
            not same_ppanggo_version
            or not same_pangbank_wf_version
            or not same_genome_metadata_sources
        ):
            raise ValueError(
                f"For collection {collection.name} release {collection_release.version}:"
                "Not the same ppanggolin_version or pangbank_wf_version or genome_metadata_sources from input file and whats in the DB.. "
                f"ppanggolin version : {collection_release.ppanggolin_version} vs {collection_release.ppanggolin_version} "
                f"ppanggolin version : {collection_release.pangbank_wf_version} vs {collection_release.pangbank_wf_version} "
                f"genome_metadata_sources : {genome_metadata_sources} vs {collection_release_from_db.genome_metadata_sources}"
            )

        collection_release = collection_release_from_db

    set_latest_release_in_collection(collection, session)

    session.commit()
    session.refresh(collection)

    return collection_release


def set_latest_release_in_collection(collection: Collection, session: Session):
    """
    Set the latest collection release for a collection.
    """
    releases = sorted(collection.releases, key=lambda x: parse(x.version), reverse=True)

    # Mark the latest release
    if releases:
        releases[0].latest = True
        logger.info(
            f"Marked release {releases[0].version} as latest for collection {collection.name}"
        )
        for release in releases[1:]:
            release.latest = False
            session.add(release)


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
    proper_open = gzip.open if genomes_md5sum_file.suffix == ".gz" else open

    with proper_open(genomes_md5sum_file, "rt") as fl:
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


def get_pangenome_metrics_from_genome_stats_summary_yaml(
    yaml_genome_stats_summary_path: Path,
):
    with open(yaml_genome_stats_summary_path, "r") as file:
        data = yaml.safe_load(file)

    pangenome_data = {
        "mean_completeness": data.get("Completeness", {}).get("mean"),
        "mean_contamination": data.get("Contamination", {}).get("mean"),
        "mean_fragmentation": data.get("Fragmentation", {}).get("mean"),
        "mean_exact_core_families_count_per_genome": data.get(
            "Exact_core_families", {}
        ).get("mean"),
        "mean_soft_core_families_count_per_genome": data.get(
            "Soft_core_families", {}
        ).get("mean"),
        "mean_persistent_families_count_per_genome": data.get(
            "Persistent_families", {}
        ).get("mean"),
        "mean_shell_families_count_per_genome": data.get("Shell_families", {}).get(
            "mean"
        ),
        "mean_cloud_families_count_per_genome": data.get("Cloud_families", {}).get(
            "mean"
        ),
    }

    return pangenome_data


def get_pangenome_metrics_from_info_yaml(yaml_info_path: Path):
    """ """

    with open(yaml_info_path, "r") as file:
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
    return pangenome_data


def get_pangenome_metrics_from_info_files(
    yaml_info_path: Path, yaml_genome_stats_summary_path: Path
) -> PangenomeMetric:
    """ """

    pangenome_data = get_pangenome_metrics_from_info_yaml(yaml_info_path)
    pangenome_data.update(
        get_pangenome_metrics_from_genome_stats_summary_yaml(
            yaml_genome_stats_summary_path
        )
    )

    return PangenomeMetric.model_validate(pangenome_data)


def add_pangenomes_to_db(
    pangenome_main_dir: Path,
    collection_release: CollectionRelease,
    genome_name_to_genome: dict[str, Genome],
    session: Session,
) -> list[Pangenome]:
    """
    Parse the pangenome directory and load pangenome data into the database.

    :param pangenome_main_dir: The main directory containing pangenome subdirectories.
    :param collection_release: The collection release to associate the pangenomes with.
    :param session: SQLAlchemy session for database transactions.
    :return: A list of Pangenome objects.
    """
    pangenomes: List[Pangenome] = []

    pangenome_dirs = [
        pangenome_dir
        for pangenome_dir in pangenome_main_dir.iterdir()
        if pangenome_dir.is_dir()
    ]
    logger.info(f"Found {len(pangenome_dirs)} pangenome directories")

    existing_pangenomes = session.exec(
        select(Pangenome).where(Pangenome.collection_release == collection_release)
    ).all()

    logger.info(f"Found {len(existing_pangenomes)} existing pangenomes in the database")

    file_to_existing_pangenome = {
        pangenome.file_name: pangenome for pangenome in existing_pangenomes
    }

    new_pangenomes: List[Pangenome] = []

    for pangenome_dir in track(
        pangenome_dirs,
        description="Parsing pangenome directories",
    ):
        pangenome_file = pangenome_dir / "pangenome.h5"
        genomes_md5sum_file = (
            pangenome_dir / "genomes_md5sum.tsv.gz"
            if (pangenome_dir / "genomes_md5sum.tsv.gz").exists()
            else pangenome_dir / "genomes_md5sum.tsv"
        )
        pangenome_info_file = pangenome_dir / "info.yaml"
        yaml_genome_stats_summary_file = (
            pangenome_dir / "genomes_statistics_summary.yaml"
        )
        genomes_statistics_file = pangenome_dir / "genomes_statistics.tsv.gz"

        # genomes_metadata_dir = pangenome_dir / "metadata"

        pangenome_local_path = Path(pangenome_file.parent.name) / pangenome_file.name

        pangenome = file_to_existing_pangenome.get(
            pangenome_local_path.as_posix(), None
        )
        if pangenome is None:
            pangenome_file_md5sum = compute_md5(pangenome_file)

            pangenome_metric = get_pangenome_metrics_from_info_files(
                pangenome_info_file, yaml_genome_stats_summary_file
            )

            pangenome = Pangenome.model_validate(
                pangenome_metric,
                from_attributes=True,
                update={
                    "file_name": pangenome_local_path.as_posix(),
                    "collection_release": collection_release,
                    "name": pangenome_dir.name,
                    "file_md5sum": pangenome_file_md5sum,
                },
            )
            new_pangenomes.append(pangenome)
            genomes = link_pangenome_and_genomes(
                pangenome=pangenome,
                genome_name_to_genome=genome_name_to_genome,
                genomes_md5sum_file=genomes_md5sum_file,
                genomes_statistics_file=genomes_statistics_file,
                session=session,
            )

            link_pangenome_and_genome_taxa(
                pangenome, genomes, collection_release.taxonomy_source, session
            )

            # metadata_files = list(
            #     genomes_metadata_dir.glob("genomes_metadata_from_*.tsv*")
            # )

            # if genomes_metadata_dir.exists() and metadata_files:
            #     add_metadata_to_genome_pangenome_links(
            #         metadata_files, pangenome_genome_links, session
            #     )
        pangenomes.append(pangenome)

    session.add_all(new_pangenomes)
    session.commit()

    logger.info(f"Added {len(new_pangenomes)} new pangenomes to the database")

    return pangenomes


def extract_source_from_metadata_file(metadata_file: Path) -> str:
    """
    Extract the source name from a metadata file with a specific naming pattern.
    """

    prefix = "genomes_metadata_from_"
    valid_suffixes = {".tsv", ".tsv.gz"}

    filename = metadata_file.name
    suffix = "".join(metadata_file.suffixes)  # Handles single and multiple suffixes

    if not filename.startswith(prefix) or suffix not in valid_suffixes:
        raise ValueError(
            f"Invalid metadata file name '{filename}'. Expected format: '{prefix}<source>.tsv' or '{prefix}<source>.tsv.gz'"
        )

    source = metadata_file.stem  # Removes the last suffix (.tsv or .gz if present)

    if source.startswith(prefix):
        source = source[len(prefix) :]  # Remove prefix

    source = source.strip()

    if not source:
        raise ValueError(
            f"Metadata file name '{filename}' does not contain a valid source name."
        )

    return source


def link_pangenome_and_genome_taxa(
    pangenome: Pangenome,
    genomes: list[Genome],
    taxonomy_source: TaxonomySource,
    session: Session,
):
    pangenome_taxa: List[Taxon] = []

    for genome in genomes:
        genome_taxa = [
            taxon for taxon in genome.taxa if taxon.taxonomy_source == taxonomy_source
        ]
        if not pangenome_taxa:
            pangenome_taxa = genome_taxa
        else:
            pangenome_taxa = get_common_taxa(genome_taxa, pangenome_taxa)
    pangenome_taxon_links = [
        PangenomeTaxonLink(pangenome_id=pangenome.id, taxon_id=taxon.id)
        for taxon in pangenome_taxa
    ]
    session.add_all(pangenome_taxon_links)


def link_pangenome_and_genomes(
    pangenome: Pangenome,
    genome_name_to_genome: dict[str, Genome],
    genomes_md5sum_file: Path,
    genomes_statistics_file: Path,
    session: Session,
):
    genome_name_to_md5sum_info = parse_genomes_hash_file(genomes_md5sum_file)
    pangenome_genome_links: List[GenomePangenomeLink] = []

    # pangenome_taxa: List[Taxon] = []
    genomes: List[Genome] = []
    for genome_metric in parse_genome_metrics_file(genomes_statistics_file):
        genome = genome_name_to_genome[genome_metric.genome_name]

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
        pangenome_genome_links.append(pangenome_genome_link)
        genomes.append(genome)
        # pangenome_taxa = get_common_taxa(genome.taxa, pangenome_taxa)

    # session.add(pangenome)
    session.add_all(pangenome_genome_links)

    return genomes
    # session.commit()

    # pangenome_taxon_links = [
    #     PangenomeTaxonLink(pangenome_id=pangenome.id, taxon_id=taxon.id)
    #     for taxon in pangenome_taxa
    # ]
    # session.add_all(pangenome_taxon_links)
    # link_pangenome_and_taxa(pangenome, pangenome_taxa, session)


def delete_full_collection(session: Session, collection_name: str) -> None:
    """
    Deletes a collection from the database if it exists.

    :param session: Database session used for querying and deleting the collection.
    :param collection_name: Name of the collection to delete.
    """
    # Query the database to find the collection with the specified name
    statement = select(Collection).where(Collection.name == collection_name)
    collection_from_db = session.exec(statement).first()

    if collection_from_db is None:
        raise ValueError(f"Collection '{collection_name}' not found in the database.")
    else:
        logger.info(f"Deleting collection '{collection_name}' from the database.")
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
        raise ValueError(
            f"Collection release '{collection_name}' (version: {release_version}) not found in the database."
        )
    else:
        logger.info(
            f"Deleting collection release '{collection_name}' (version: {release_version}) from the database."
        )
        session.delete(collection_release_from_db)
        session.commit()


def print_collections():
    """ """
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

            for collection_release in collection.releases:
                table.add_row(
                    str(collection_release.version),
                    str(len(collection_release.pangenomes)),
                    collection_release.release_note,
                )

            console.print(table)
