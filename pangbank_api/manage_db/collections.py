import csv
import gzip
import logging
from pathlib import Path
from typing import Iterator, List

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
    GenomeInPangenomeMetadataSource,
    MetadataBase,
    GenomeInPangenomeMetadata,
    TaxonomySource,
)
from pangbank_api.manage_db.genome_metadata import parse_metadata_table


logger = logging.getLogger(__name__)  # __name__ ensures uniqueness per module


def create_collection_release(
    collection_input: Collection,
    collection_release_input: CollectionRelease,
    taxonomy_source: TaxonomySource,
    session: Session,
) -> CollectionRelease:
    """
    Create or retrieve a collection release from the database.

    This function:
    - Ensures the collection exists in the database, creating it if necessary.
    - Ensures the collection release exists in the database, creating it if necessary.
    - Validates version consistency between the input file and existing database records.
    """

    collection = Collection.model_validate(
        collection_input, update={"taxonomy_source": taxonomy_source}
    )

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

    existing_pangenomes = session.exec(
        select(Pangenome).where(Pangenome.collection_release == collection_release)
    ).all()

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
        genomes_statistics_file = pangenome_dir / "genomes_statistics.tsv.gz"

        genomes_metadata_dir = pangenome_dir / "metadata"

        pangenome_local_path = Path(pangenome_file.parent.name) / pangenome_file.name

        pangenome = file_to_existing_pangenome.get(
            pangenome_local_path.as_posix(), None
        )
        if pangenome is None:

            pangenome_metric = get_pangenome_metrics_from_info_file(pangenome_info_file)
            pangenome = Pangenome.model_validate(
                pangenome_metric,
                from_attributes=True,
                update={
                    "file_name": pangenome_local_path.as_posix(),
                    "collection_release": collection_release,
                },
            )
            new_pangenomes.append(pangenome)
            # session.add(pangenome)

            genomes, pangenome_genome_links = link_pangenome_and_genomes(
                pangenome=pangenome,
                genome_name_to_genome=genome_name_to_genome,
                genomes_md5sum_file=genomes_md5sum_file,
                genomes_statistics_file=genomes_statistics_file,
                session=session,
            )
            # session.commit()
            # session.refresh(pangenome)

            link_pangenome_and_genome_taxa(pangenome, genomes, session)

            metadata_files = list(
                genomes_metadata_dir.glob("genomes_metadata_from_*.tsv*")
            )

            if genomes_metadata_dir.exists() and metadata_files:
                add_metadata_to_genome_pangenome_links(
                    metadata_files, pangenome_genome_links, session
                )

        pangenomes.append(pangenome)

    session.add_all(new_pangenomes)

    session.commit()

    return pangenomes


def add_metadata_to_genome_pangenome_links(
    metadata_files: List[Path],
    pangenome_genome_links: list[GenomePangenomeLink],
    session: Session,
):
    genome_name_to_link = {link.genome.name: link for link in pangenome_genome_links}

    for metadata_file in metadata_files:

        source_name = extract_source_from_metadata_file(metadata_file)

        metadata_source = add_metadata_source_to_db(source_name, session)

        metadatas: List[GenomeInPangenomeMetadata] = []
        genome_to_metadata_list = parse_metadata_table(
            metadata_file, disable_track=True
        )

        for genome_name, metadata_list in genome_to_metadata_list:
            link = genome_name_to_link.get(genome_name, None)
            if link:
                metadatas += create_metadata(link, metadata_list, metadata_source)

        session.add_all(metadatas)

    session.commit()


def add_metadata_source_to_db(metadata_source_name: str, session: Session):

    metadata_source = session.exec(
        select(GenomeInPangenomeMetadataSource).where(
            (GenomeInPangenomeMetadataSource.name == metadata_source_name)
        )
    ).first()

    if metadata_source is None:
        metadata_source = GenomeInPangenomeMetadataSource(name=metadata_source_name)

        logger.info(f"Adding metadata source '{metadata_source_name}' to the database")

        session.add(metadata_source)
        session.commit()
        session.refresh(metadata_source)
    # else:
    #     logger.debug(
    #         f"Metadata source '{metadata_source_name}' already exists in the database"
    #     )

    return metadata_source


def create_metadata(
    genome_in_pangenome: GenomePangenomeLink,
    metadata_list: list[MetadataBase],
    source: GenomeInPangenomeMetadataSource,
):
    """ """
    metadatas: List[GenomeInPangenomeMetadata] = []

    for metadata_input in metadata_list:
        metadata = GenomeInPangenomeMetadata.model_validate(
            metadata_input,
            update={"genome_in_pangenome": genome_in_pangenome, "source": source},
        )
        metadatas.append(metadata)

    return metadatas


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
    pangenome: Pangenome, genomes: list[Genome], session: Session
):

    pangenome_taxa: List[Taxon] = []

    for genome in genomes:
        if not pangenome_taxa:
            pangenome_taxa = genome.taxa
        else:
            pangenome_taxa = get_common_taxa(genome.taxa, pangenome_taxa)
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

    return genomes, pangenome_genome_links
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
