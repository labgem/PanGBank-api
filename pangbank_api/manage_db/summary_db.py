from pangbank_api.models import Collection, CollectionRelease, PanGBankSummary, Pangenome, GenomePangenomeLink, Taxon, PangenomeTaxonLink

from rich import pretty

from sqlalchemy import func
from sqlmodel import Session, select


RANK_MAPPING = {
    "species": ["species", "Species", "S"],
    "genus": ["genus", "Genus", "G"],
    "family": ["family", "Family", "F"],
    "order": ["order", "Order", "O"],
    "class": ["class", "Class", "C"],
    "phylum": ["phylum", "Phylum", "P"],
    "domain": ["domain", "Domain", "D"],
}

def count_taxa_by_rank(session: Session, rank: str) -> int:

    return session.exec(
        select(func.count(func.distinct(Taxon.id)))
        .select_from(CollectionRelease)
        .join(Pangenome)
        .join(PangenomeTaxonLink)
        .join(Taxon)
        .where(
            CollectionRelease.latest == True,
            Taxon.rank.in_(RANK_MAPPING[rank]),  # type: ignore
        )
    ).one()




def get_database_statistics(session: Session) -> PanGBankSummary:

    collection_count = session.exec(
        select(func.count())
        .select_from(Collection)
    ).one()

    release_count = session.exec(
        select(func.count())
        .select_from(CollectionRelease)
    ).one()

    # Only latest release of each collection
    pangenome_count = session.exec(
            select(func.count())
            .select_from(Pangenome)
        .join(CollectionRelease)
        .where(CollectionRelease.latest == True)
    ).one()

    # Unique genomes across latest releases
    genome_count = session.exec(
        select(func.count(func.distinct(GenomePangenomeLink.genome_id)))
        .select_from(CollectionRelease)
        .join(Pangenome)
        .join(GenomePangenomeLink)
        .where(CollectionRelease.latest == True)
    ).one()

    taxonomy_counts = {
        "domain": count_taxa_by_rank(session, "domain"),
        "phylum": count_taxa_by_rank(session, "phylum"),
        "class": count_taxa_by_rank(session, "class"),
        "order": count_taxa_by_rank(session, "order"),
        "family": count_taxa_by_rank(session, "family"),
        "genus": count_taxa_by_rank(session, "genus"),
        "species": count_taxa_by_rank(session, "species"),
    }

    summary_record = PanGBankSummary(
        collection_count=collection_count,
        release_count=release_count,
        pangenome_count=pangenome_count,
        genome_count=genome_count,
        species_count=taxonomy_counts["species"],
        genus_count=taxonomy_counts["genus"],
        family_count=taxonomy_counts["family"],
        order_count=taxonomy_counts["order"],
        class_count=taxonomy_counts["class"],
        phylum_count=taxonomy_counts["phylum"],
        domain_count=taxonomy_counts["domain"],
    )

    return summary_record


def update_database_statistics(session: Session):
    """
    Update the existing database statistics in the pangbanksummary table.
    """

    summary = get_database_statistics(session)
    summary.id = 1

    session.merge(summary)
    session.commit()

    pretty.pprint("PanGBank database summary:")
    pretty.pprint(summary.model_dump())

