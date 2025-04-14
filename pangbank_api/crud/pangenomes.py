from pathlib import Path
from typing import Iterator, Sequence

from sqlalchemy import func

from sqlalchemy.orm import aliased

from sqlmodel import Session, select

from pangbank_api.crud.common import (
    FilterGenomeTaxonGenomePangenome,
    PaginationParams,
    FilterGenome,
    FilterGenomeMetadata,
    get_taxonomies_from_taxa,
)
from pangbank_api.models import (
    Genome,
    GenomePangenomeLink,
    Pangenome,
    PangenomePublic,
    PangenomeTaxonLink,
    Taxon,
    CollectionRelease,
    CollectionReleasePublic,
    GenomeInPangenomeMetadata,
    Collection,
)


def get_pangenome_file(session: Session, pangenome_id: int) -> Path | None:
    pangenome = session.get(Pangenome, pangenome_id)

    if not pangenome:
        return None

    pangenome_file = (
        Path(pangenome.collection_release.pangenomes_directory) / pangenome.file_name
    )

    return pangenome_file


def get_pangenome(session: Session, pangenome_id: int) -> Pangenome | None:
    pangenome = session.get(Pangenome, pangenome_id)
    if pangenome is None:
        return None

    return pangenome


def make_pangenome_public(pangenome: Pangenome) -> PangenomePublic:

    taxonomies = get_taxonomies_from_taxa(pangenome.taxa)

    assert (
        len(taxonomies) == 1
    ), f"{pangenome.file_name} {pangenome.id} have an issue with its taxonomies. Found {len(taxonomies)} taxonomies"

    collection_release_public = CollectionReleasePublic(
        **pangenome.collection_release.model_dump(),
        taxonomy_source=pangenome.collection_release.taxonomy_source,
        collection_name=pangenome.collection_release.collection.name,
        collection=pangenome.collection_release.collection
    )

    pangenome_public = PangenomePublic(
        **pangenome.model_dump(),
        collection_release=collection_release_public,
        taxonomy=taxonomies[0],
    )
    
    return pangenome_public


def get_public_pangenome(session: Session, pangenome_id: int) -> PangenomePublic | None:

    pangenome = get_pangenome(session, pangenome_id)
    if pangenome is None:
        return None

    return make_pangenome_public(pangenome)


def get_pangenomes(
    session: Session,
    filter_params: FilterGenomeTaxonGenomePangenome | None = None,
    pagination_params: PaginationParams | None = None,
) -> Sequence[Pangenome]:

    query = select(Pangenome).distinct()

    if filter_params and filter_params.pangenome_name is not None:
        query = query.where(Pangenome.name == filter_params.pangenome_name)

    collectionrelease_alias = aliased(CollectionRelease)
    # collectionrelease_alias_2 = aliased(CollectionRelease)

    if filter_params and filter_params.only_latest_release is True:
        query = query.join(collectionrelease_alias).where(
            collectionrelease_alias.latest
        )

    if filter_params and filter_params.collection_name is not None:
        query = (
            query.join(CollectionRelease)
            .join(Collection)
            .where(Collection.name == filter_params.collection_name)
        )

    if filter_params and filter_params.collection_id is not None:
        query = (
            query.join(CollectionRelease)
            .join(Collection)
            .where(Collection.id == filter_params.collection_id)
        )

    if filter_params and filter_params.genome_name is not None:
        query = (
            query.join(GenomePangenomeLink)
            .join(Genome)
            .where(Genome.name == filter_params.genome_name)
        )

    if filter_params and filter_params.taxon_name is not None:
        # Apply offset and limit

        query = query.join(PangenomeTaxonLink).join(Taxon)
        if filter_params.substring_taxon_match:
            query = query.where(
                func.lower(Taxon.name).like(f"%{filter_params.taxon_name.lower()}%")
            )
        else:
            # exact match
            query = query.where(Taxon.name == filter_params.taxon_name)
    # Apply offset and limit
    if pagination_params:
        query = query.offset(pagination_params.offset).limit(pagination_params.limit)

    pangenomes = session.exec(query).all()

    return pangenomes


def get_public_pangenomes(
    session: Session,
    filter_params: FilterGenomeTaxonGenomePangenome | None = None,
    pagination_params: PaginationParams | None = None,
) -> Iterator[PangenomePublic]:

    pangenomes = get_pangenomes(session, filter_params, pagination_params)

    public_pangenomes = (make_pangenome_public(pangenome) for pangenome in pangenomes)

    return public_pangenomes


def get_genomes_in_pangenome(
    session: Session,
    pangenome_id: int,
    filter_genome: FilterGenome | None = None,
    filter_metadata: FilterGenomeMetadata | None = None,
    pagination_params: PaginationParams | None = None,
):
    # Alias for the metadata table
    metadata_alias = aliased(GenomeInPangenomeMetadata)

    query = (
        select(GenomePangenomeLink)
        .distinct()
        .join(Pangenome)
        .where(Pangenome.id == pangenome_id)
    )

    if filter_genome and filter_genome.genome_name is not None:
        query = query.join(Genome).where(Genome.name == filter_genome.genome_name)

    if pagination_params:
        query = query.offset(pagination_params.offset).limit(pagination_params.limit)

    if filter_metadata:
        if filter_metadata.metadata_key:
            query = query.join(metadata_alias).where(
                metadata_alias.key == filter_metadata.metadata_key
            )

        if filter_metadata.metadata_value is not None:
            query = query.where(metadata_alias.value == filter_metadata.metadata_value)

    pangenome_genomes_links = session.exec(query).all()
    return pangenome_genomes_links


def get_genome_in_pangenome(session: Session, pangenome_id: int, genome_id: int):

    query = (
        select(GenomePangenomeLink)
        .distinct()
        .where(
            (GenomePangenomeLink.pangenome_id == pangenome_id)
            & (GenomePangenomeLink.genome_id == genome_id)
        )
    )

    pangenome_genome_link = session.exec(query).first()
    return pangenome_genome_link
