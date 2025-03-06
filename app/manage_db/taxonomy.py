import gzip
import logging
from pathlib import Path

import typer
from rich.progress import Progress, track
from sqlmodel import Session, select

from app.models import (
    Genome,
    GenomeTaxonLink,
    Taxon,
    TaxonomySource,
    TaxonomySourceInput,
)

app = typer.Typer(no_args_is_help=True)


def parse_taxonomy_file(taxonomy_file: Path) -> dict[str, tuple[str, ...]]:

    genome_to_lineage: dict[str, tuple[str, ...]] = {}

    proper_open = gzip.open if taxonomy_file.suffix == ".gz" else open

    with proper_open(taxonomy_file, "rt") as fl:  # 'rt' mode to read as text
        for line in fl:
            genome_name, taxonomy_str = line.strip().split("\t")
            genome_to_lineage[genome_name] = tuple(
                name.strip() for name in taxonomy_str.split(";")
            )

    return genome_to_lineage


def parse_ranks_str(ranks_str: str) -> list[str]:

    ranks = [rank.strip().title() for rank in ranks_str.split(";")]

    return ranks


def create_taxonomy_source(
    taxonomy_source_input: TaxonomySourceInput, session: Session
) -> TaxonomySource:

    # Check if taxonomy release already exists in DB
    statement = select(TaxonomySource).where(
        (TaxonomySource.name == taxonomy_source_input.name)
        & (TaxonomySource.version == taxonomy_source_input.version)
    )

    taxonomy_source = session.exec(statement).first()

    if taxonomy_source is None:
        logging.info(
            f"Taxonomy source '{taxonomy_source_input.name}' (version {taxonomy_source_input.version}) not found in the database. Adding it now."
        )
        taxonomy_source = TaxonomySource.model_validate(taxonomy_source_input)

        session.add(taxonomy_source)

        session.commit()
        session.refresh(taxonomy_source)

    else:
        # the taxonomy release exists already
        # checking that given ranks are identical that ones attached to the taxonomy release

        if parse_ranks_str(taxonomy_source.ranks) != parse_ranks_str(
            taxonomy_source_input.ranks
        ):
            raise ValueError(
                f"Discrepancy in ranks for taxonomy_source {taxonomy_source}. "
                f"Existing ranks : {parse_ranks_str(taxonomy_source.ranks)} vs given ranks {parse_ranks_str(taxonomy_source_input.ranks)}"
            )

        logging.info(
            f"taxonomy_source {taxonomy_source_input.name}:{taxonomy_source_input.version} already exist in DB"
        )

    return taxonomy_source


def get_common_taxa(taxa_A: list[Taxon], taxa_B: list[Taxon]) -> list[Taxon]:

    common_taxa: list[Taxon] = []
    for taxon in taxa_A:
        if taxon in taxa_B:
            common_taxa.append(taxon)

    return common_taxa


def get_taxa_by_depth(depth: int, taxonomy_source: TaxonomySource, session: Session):

    statement = select(Taxon).where(
        (Taxon.depth == depth) & (Taxon.taxonomy_source == taxonomy_source)
    )
    taxa = session.exec(statement).all()

    return taxa


def create_taxon_from_lineages(
    ranks: list[str],
    lineages: set[tuple[str, ...]],
    taxonomy_source: TaxonomySource,
    session: Session,
):

    taxon_names_by_depths: list[set[str]] = [set() for _ in range(len(ranks))]
    new_taxa: list[Taxon] = []
    taxa: list[Taxon] = []

    for lineage in lineages:
        for i, taxon_name in enumerate(lineage):
            taxon_names_by_depths[i].add(taxon_name)

    taxa_count = sum(len(taxon_set) for taxon_set in taxon_names_by_depths)

    name_to_taxon_by_depth: list[dict[str, Taxon]] = []

    with Progress() as progress:

        progress_task = progress.add_task("Creating taxa", total=taxa_count)

        for depth, taxon_set in enumerate(taxon_names_by_depths):

            taxon_name_to_taxon: dict[str, Taxon] = {}
            name_to_taxon_by_depth.append(taxon_name_to_taxon)

            name_to_taxon_at_depth = {
                taxon.name: taxon
                for taxon in get_taxa_by_depth(depth, taxonomy_source, session)
            }

            for taxon_name in taxon_set:

                try:

                    taxon = name_to_taxon_at_depth[taxon_name]
                except KeyError:

                    taxon = Taxon(
                        name=taxon_name,
                        rank=ranks[depth],
                        depth=depth,
                    )
                    new_taxa.append(taxon)

                taxa.append(taxon)
                taxon_name_to_taxon[taxon.name] = taxon

                progress.update(progress_task, advance=1)

    logging.info(f"Created {len(new_taxa)} new taxa out of {len(taxa)} total taxa.")
    session.add_all(new_taxa)

    for new_taxon in new_taxa:
        new_taxon.taxonomy_source = taxonomy_source

    session.refresh(taxonomy_source)
    session.commit()

    return name_to_taxon_by_depth


def link_genomes_and_taxa(
    genome_name_to_genome: dict[str, Genome],
    genome_name_to_lineage: dict[
        str,
        tuple[str, ...],
    ],
    name_to_taxon_by_depth: list[dict[str, Taxon]],
    session: Session,
):

    new_genome_taxon_links: list[GenomeTaxonLink] = []
    logging.info(f"Linking {len(genome_name_to_genome)} genomes.")

    linked_genome_count = 0
    unlinked_genomes_count = 0
    for genome_name, genome in track(
        genome_name_to_genome.items(), "Linking genomes to taxa"
    ):

        lineage = genome_name_to_lineage[genome_name]

        taxon_name = lineage[0]
        taxon = name_to_taxon_by_depth[0][taxon_name]

        existing_link = session.get(GenomeTaxonLink, (genome.id, taxon.id))
        if existing_link is None:
            unlinked_genomes_count += 1
            for depth, taxon_name in enumerate(lineage):

                taxon = name_to_taxon_by_depth[depth][taxon_name]

                new_genome_taxon_links.append(
                    GenomeTaxonLink(genome_id=genome.id, taxon_id=taxon.id)
                )
        else:
            linked_genome_count += 1

    logging.info(
        f"{linked_genome_count} genomes were already linked in the GenomeTaxonLink table."
    )
    logging.info(
        f"{unlinked_genomes_count} genomes were not linked in the GenomeTaxonLink table."
    )
    logging.info(
        f"Adding {len(new_genome_taxon_links)} new GenomeTaxonLink entries to the database."
    )
    session.add_all(new_genome_taxon_links)

    session.commit()


def add_taxon_to_db(
    taxonomy_source: TaxonomySource,
    lineages: set[tuple[str, ...]],
    session: Session,
):

    ranks = [rank.strip() for rank in taxonomy_source.ranks.split(";")]

    logging.info(
        f"Adding taxa from taxonomy source '{taxonomy_source.name}' to the database."
    )
    name_to_taxon_by_depth = create_taxon_from_lineages(
        ranks=ranks,
        lineages=lineages,
        taxonomy_source=taxonomy_source,
        session=session,
    )
    logging.info(
        f"Completed adding taxa from taxonomy source '{taxonomy_source.name}' to the database."
    )
    return name_to_taxon_by_depth
