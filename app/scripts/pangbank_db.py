import typer

from app.scripts.collections import (
    app as collection_app,
    add_pangenomes_to_db,
    create_collection_release,
)
from app.scripts.genome_metadata import app as genome_metadata_app
from app.scripts.taxonomy import (
    add_taxon_to_db,
    link_genomes_and_taxa,
)
from app.scripts.genomes import app as genome_app, add_genomes_to_db

from pathlib import Path
import logging

from typing_extensions import Annotated

from app.scripts.taxonomy import parse_taxonomy_file


from app.database import create_db_and_tables, engine


from app.scripts.utils import check_collection_release_input_json

from rich.logging import RichHandler

from sqlmodel import Session

cli = typer.Typer(no_args_is_help=True)


cli.add_typer(collection_app, name="collections")

cli.add_typer(genome_metadata_app, name="genome_metadata")

cli.add_typer(genome_app, name="genomes")


@cli.command(no_args_is_help=True)
def add_pangenomes(
    collection_release_json: Annotated[
        Path, typer.Argument(help="Path to the collection release input json file.")
    ]
):
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[RichHandler()],
    )

    create_db_and_tables()

    data_input = check_collection_release_input_json(collection_release_json)

    collection_input = data_input.collection
    collection_release_input = data_input.release
    taxonomy_input = data_input.taxonomy

    taxonomy_file = taxonomy_input.file

    genome_sources = data_input.genome_sources

    pangenome_dir = (
        collection_release_json.parent / collection_release_input.pangenomes_directory
    )

    genome_name_to_lineage = parse_taxonomy_file(taxonomy_file)
    lineages = set(genome_name_to_lineage.values())

    create_db_and_tables()

    with Session(engine) as session:

        genome_name_to_genome = add_genomes_to_db(genome_sources, session)

        name_to_taxon_by_depth = add_taxon_to_db(
            taxonomy_input,
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
            session=session,
        )

        add_pangenomes_to_db(
            pangenome_dir,
            genome_name_to_genome=genome_name_to_genome,
            collection_release=collection_release,
            session=session,
        )


if __name__ == "__main__":

    cli()
