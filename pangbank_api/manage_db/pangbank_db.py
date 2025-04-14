import logging
from pathlib import Path

from typing import Optional

import typer
from sqlmodel import Session
from typing_extensions import Annotated

from pangbank_api.database import create_db_and_tables, engine
from pangbank_api.manage_db.collections import (
    add_pangenomes_to_db,
    create_collection_release,
    delete_collection_release,
    delete_full_collection,
    print_collections,
)
from pangbank_api.manage_db.genome_metadata import app as genome_metadata_app
from pangbank_api.manage_db.genomes import add_genomes_to_db
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


@cli.command()
def list_collections():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    create_db_and_tables()

    print_collections()


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


if __name__ == "__main__":
    cli()
