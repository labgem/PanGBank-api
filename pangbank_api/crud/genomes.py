from sqlalchemy import func
from sqlmodel import Session, select

from pangbank_api.crud.common import (
    FilterGenomeTaxon,
    PaginationParams,
    get_taxonomies_from_taxa,
)
from pangbank_api.models import (
    Genome,
    GenomePublic,
    GenomeTaxonLink,
    Taxon,
    TaxonomyPublic,
    GenomeSourcePublic,
    GenomeStatusPublic,
)


def get_genome_public(genome: Genome) -> GenomePublic:
    taxonomies = get_taxonomies_from_taxa(genome.taxa)

    # Convert genome statuses to public models
    statuses = [
        GenomeStatusPublic(
            id=(
                status.id if status.id is not None else 0
            ),  # id should always be set from DB
            status_type=status.status_type,
            origin=status.origin,
            collection_release_id=status.collection_release_id,
        )
        for status in genome.statuses
    ]

    genome_public = GenomePublic(
        **genome.model_dump(),
        taxonomies=[TaxonomyPublic(**taxonomy.model_dump()) for taxonomy in taxonomies],
        genome_source=GenomeSourcePublic(**genome.genome_source.model_dump()),
        statuses=statuses,
    )
    return genome_public


def get_genome_by_id(session: Session, genome_id: int) -> GenomePublic | None:
    genome = session.get(Genome, genome_id)
    if genome is None:
        return None

    return get_genome_public(genome)


def get_genome_by_name(session: Session, genome_name: str) -> GenomePublic | None:
    genome = session.exec(select(Genome).where(Genome.name == genome_name)).first()

    if genome is None:
        return None

    return get_genome_public(genome)


def get_genomes(
    session: Session,
    filter_params: FilterGenomeTaxon,
    pagination_params: PaginationParams | None,
) -> list[GenomePublic]:
    query = select(Genome).distinct()

    if filter_params.genome_name is not None:
        query = query.where(Genome.name == filter_params.genome_name)

    if filter_params.taxon_name is not None:
        # Apply offset and limit

        query = query.join(GenomeTaxonLink).join(Taxon)

        if filter_params.substring_taxon_match:
            query = query.where(
                func.lower(Taxon.name).like(f"%{filter_params.taxon_name.lower()}%")
            )
        else:
            # exact match
            query = query.where(Taxon.name == filter_params.taxon_name)

    if pagination_params:
        query = query.offset(pagination_params.offset).limit(pagination_params.limit)

    db_genomes = session.exec(query).all()

    public_genomes = [get_genome_public(genome) for genome in db_genomes]

    return public_genomes
