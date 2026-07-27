import logging
from pathlib import Path

from typing import Optional

import typer
from sqlmodel import Session, select
from typing import Annotated

from pangbank_api.database import create_db_and_tables, engine
from pangbank_api.manage_db.collections import (
    add_pangenomes_to_db,
    create_collection_release,
    delete_collection_release,
    delete_full_collection,
    print_collections,
    update_collection_release_counts,
)

from pangbank_api.manage_db.summary_db import update_database_statistics


from pangbank_api.manage_db.genome_metadata import (
    update_genomes_with_quality_metrics,
    app as genome_metadata_app,
    parse_metadata_table,
    get_valid_genome_quality_columns,
)
from pangbank_api.manage_db.genome_status import add_genome_statuses_to_release
from pangbank_api.manage_db.genomes import add_genomes_to_db
from pangbank_api.manage_db.input_models import GenomeStatusInput
from pangbank_api.manage_db.taxonomy import (
    add_taxon_to_db,
    link_genomes_and_taxa,
    parse_taxonomy_file,
    create_taxonomy_source,
)
from pangbank_api.manage_db.utils import (
    parse_collection_release_input_json,
    set_up_logging_config,
)
from pangbank_api.models import Collection, CollectionRelease, Genome

cli = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
    short_help="Manage pangbank database.",
    context_settings={"help_option_names": ["-h", "--help"]},
)


cli.add_typer(
    genome_metadata_app, name="genome-metadata", short_help="Manage genome metadata."
)


@cli.command(no_args_is_help=True)
def add_collection_release(
    collection_release_json: Path = typer.Argument(
        ...,
        help="Path to the collection release input json file.",
        exists=True,
        dir_okay=True,
    ),
    pangbank_data_dir: Annotated[
        Path,
        typer.Option(
            envvar="PANGBANK_DATA_DIR",
            help="Path to the pangbank data directory.",
            exists=True,
        ),
    ] = Path("./"),
):
    set_up_logging_config()

    data_input = parse_collection_release_input_json(
        collection_release_json, pangbank_data_dir
    )

    collection_input = data_input.collection
    collection_release_input = data_input.release
    taxonomy_input = data_input.taxonomy
    genome_metadata_input = data_input.genome_metadata
    genome_status_inputs = data_input.genome_statuses

    taxonomy_file = taxonomy_input.file

    genome_sources = data_input.genome_sources

    pangenome_dir = pangbank_data_dir / collection_release_input.pangenomes_directory

    genome_name_to_lineage = parse_taxonomy_file(taxonomy_file)
    lineages = set(genome_name_to_lineage.values())

    create_db_and_tables()

    with Session(engine) as session:
        genome_name_to_genome = add_genomes_to_db(genome_sources, session)

        taxonomy_source = create_taxonomy_source(taxonomy_input, session=session)

        name_to_taxon_by_depth = add_taxon_to_db(
            taxonomy_source,
            lineages,
            session,
        )

        link_genomes_and_taxa(
            genome_name_to_genome,
            genome_name_to_lineage,
            name_to_taxon_by_depth,
            session,
        )

        collection_release = create_collection_release(
            collection_input=collection_input,
            collection_release_input=collection_release_input,
            taxonomy_source=taxonomy_source,
            session=session,
        )

        add_pangenomes_to_db(
            pangenome_dir,
            genome_name_to_genome=genome_name_to_genome,
            collection_release=collection_release,
            session=session,
        )

        # Update the cached counts after all pangenomes have been added
        update_collection_release_counts(collection_release, session)

        # Update Genome table with genome metadata and assembly statistics
        if genome_metadata_input is not None:
            logging.info(
                f"Processing genome metadata from {genome_metadata_input.file}"
            )
            # Get valid optional columns from the Genome model to filter during parsing
            valid_columns = get_valid_genome_quality_columns()

            # Pass the generator directly - saves memory by not materializing entire dict
            genome_quality_metrics_generator = parse_metadata_table(
                genome_metadata_input.file,
                valid_columns=valid_columns,
            )

            update_genomes_with_quality_metrics(
                genome_quality_metrics_generator,
                session=session,
                collection_release=collection_release,
                allow_overwrite=True,  # Initial import - allow setting all values
            )

        # Add genome status information (representative/reference genomes)
        add_genome_statuses_to_release(
            collection_release,
            genome_status_inputs,
            genome_name_to_genome,
            session=session,
        )

        update_database_statistics(session=session)


@cli.command()
def list_collections():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    create_db_and_tables()

    print_collections()


@cli.command()
def compute_database_statistics():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    set_up_logging_config()
    create_db_and_tables()

    with Session(engine) as session:
        update_database_statistics(session=session)


@cli.command(no_args_is_help=True)
def delete_collection(
    collection_name: Annotated[
        str, typer.Argument(help="Name of the collection to delete.")
    ],
    release_version: Annotated[
        Optional[str],
        typer.Option(
            help="Specific release version to delete. If not provided, the entire collection will be deleted."
        ),
    ] = None,
):
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    create_db_and_tables()

    with Session(engine) as session:
        if release_version:
            delete_collection_release(session, collection_name, release_version)

        else:
            delete_full_collection(session, collection_name)

        update_database_statistics(session=session)

@cli.command(no_args_is_help=True)
def add_genome_statuses(
    collection_name: Annotated[str, typer.Option(help="Name of the collection.")],
    release_version: Annotated[
        str, typer.Option(help="Version of the collection release.")
    ],
    status_type: Annotated[
        str,
        typer.Option(
            help="Type of genome status (e.g., 'representative', 'reference', 'type_strain')."
        ),
    ],
    origin: Annotated[
        str,
        typer.Option(help="Origin of the genome status (e.g., 'GTDB', 'NCBI_RefSeq')."),
    ],
    file: Annotated[
        Path,
        typer.Option(
            help="Path to text file containing genome names (one per line).",
            exists=True,
        ),
    ],
):
    """
    Add genome status information to an existing collection release.

    This command allows you to add or update genome statuses (representative, reference, type strain, etc.)
    for genomes in an existing collection release without re-importing the entire collection.
    """
    set_up_logging_config()

    create_db_and_tables()

    with Session(engine) as session:
        # Find the collection release
        statement = (
            select(CollectionRelease)
            .join(Collection)
            .where(
                (Collection.name == collection_name)
                & (CollectionRelease.version == release_version)
            )
        )

        collection_release = session.exec(statement).first()

        if collection_release is None:
            logging.error(
                f"Collection release not found: {collection_name}:{release_version}"
            )
            raise typer.Exit(code=1)

        logging.info(f"Found collection release: {collection_name}:{release_version}")

        # TODO: The add-genome-statuses CLI command loads all genomes into memory (select(Genome)).all())
        # to build a name->genome map. On large databases this can be slow and memory-heavy.
        # Consider reading the status file first and querying only the needed genomes (e.g., WHERE Genome.name IN (...)),
        # or iterating in chunks.

        # Get all genomes from the database
        all_genomes = session.exec(select(Genome)).all()
        genome_name_to_genome = {genome.name: genome for genome in all_genomes}

        logging.info(f"Found {len(genome_name_to_genome)} genomes in the database")

        # Create GenomeStatusInput
        genome_status_input = GenomeStatusInput(
            status_type=status_type,
            origin=origin,
            file=file,
        )

        # Add genome statuses
        add_genome_statuses_to_release(
            collection_release=collection_release,
            genome_status_inputs=[genome_status_input],
            genome_name_to_genome=genome_name_to_genome,
            session=session,
        )

        logging.info(
            f"Successfully processed genome statuses for {collection_name}:{release_version}"
        )


@cli.command(no_args_is_help=True)
def add_quality_metrics(
    file: Annotated[
        Path,
        typer.Argument(
            help="Path to TSV file containing genome quality metrics (e.g., CheckM results). "
            "Must have a 'genomes' column and quality metric columns matching Genome model fields.",
            exists=True,
        ),
    ],
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Allow overwriting existing quality metric values. "
            "By default, attempting to change existing values raises an error. "
            "Use this flag to intentionally update values (warnings will be logged).",
        ),
    ] = False,
):
    """
    Add genome quality metrics to genomes in the database.

    This command allows you to add or update genome quality metrics (CheckM completeness,
    contamination, genome size, etc.) for genomes that already exist in the database.

    The TSV file should contain:
    - A 'genomes' column with genome names
    - Quality metric columns (e.g., checkm2_completeness, checkm2_contamination, genome_size, etc.)

    Only columns that match optional fields in the Genome model will be imported.
    Invalid columns will be automatically filtered out.

    By default, the command will fail if trying to change existing values.
    Use --force to allow overwriting (with warnings).

    Examples:
        # Add quality metrics (fails if values already exist and differ)
        pangbank_db add-quality-metrics genome_metadata.tsv

        # Force update existing values
        pangbank_db add-quality-metrics genome_metadata.tsv --force
    """
    set_up_logging_config()

    create_db_and_tables()

    with Session(engine) as session:
        # Get valid optional columns from the Genome model to filter during parsing
        logging.info("Processing genome quality metrics...")
        valid_columns = get_valid_genome_quality_columns()
        logging.info(f"Valid quality metric columns: {sorted(valid_columns)}")

        # Parse the quality metrics file with column filtering
        genome_quality_metrics_generator = parse_metadata_table(
            file,
            valid_columns=valid_columns,
        )

        # Update all genomes with quality metrics (collection_release=None means all genomes)
        update_genomes_with_quality_metrics(
            genome_quality_metrics_generator,
            session=session,
            collection_release=None,
            allow_overwrite=force,
        )

        logging.info("Successfully processed genome quality metrics")


if __name__ == "__main__":
    cli()
