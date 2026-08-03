from collections import defaultdict
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any, cast

from sqlalchemy import func
from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from pangbank_api.crud.common import (
    FilterGenome,
    FilterGenomeMetadata,
    FilterGenomeTaxonGenomePangenome,
    PaginationParams,
    get_taxonomies_from_taxa,
)
from pangbank_api.models import (
    Collection,
    CollectionPublic,
    CollectionRelease,
    CollectionReleasePublic,
    Genome,
    GenomePangenomeLink,
    Pangenome,
    PangenomePublic,
    PangenomeTaxonLink,
    Taxon,
    TaxonomyPublic,
    TaxonomySourcePublic,
)

def _build_pangenomes_query(
    filter_params: FilterGenomeTaxonGenomePangenome | None = None,
):
    query = select(Pangenome).distinct()

    if filter_params and filter_params.pangenome_name is not None:
        query = query.where(Pangenome.name == filter_params.pangenome_name)

    needs_release_join = bool(
        filter_params
        and (
            filter_params.release_version is not None
            or filter_params.only_latest_release
            or filter_params.collection_name is not None
            or filter_params.collection_id is not None
        )
    )
    if needs_release_join:
        query = query.join(CollectionRelease)

    if filter_params and filter_params.release_version is not None:
        query = query.where(CollectionRelease.version == filter_params.release_version)

    if filter_params and filter_params.only_latest_release is True:
        query = query.where(CollectionRelease.latest)

    if filter_params and (
        filter_params.collection_name is not None
        or filter_params.collection_id is not None
    ):
        query = query.join(Collection)
        if filter_params.collection_name is not None:
            query = query.where(Collection.name == filter_params.collection_name)
        if filter_params.collection_id is not None:
            query = query.where(Collection.id == filter_params.collection_id)

    if filter_params and filter_params.genome_name is not None:
        query = (
            query.join(GenomePangenomeLink)
            .join(Genome)
            .where(Genome.name == filter_params.genome_name)
        )

    if filter_params and filter_params.taxon_name is not None:
        query = query.join(PangenomeTaxonLink).join(Taxon)
        if filter_params.substring_taxon_match:
            query = query.where(
                func.lower(Taxon.name).like(f"%{filter_params.taxon_name.lower()}%")
            )
        else:
            query = query.where(Taxon.name == filter_params.taxon_name)

    return query


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


def make_pangenome_public_metrics(p: Pangenome) -> dict[str, float]:
    mean_fam_per_genome = p.mean_persistent_families_count_per_genome + p.mean_shell_families_count_per_genome + p.mean_cloud_families_count_per_genome
    return {
        "persistent_fraction": p.mean_persistent_families_count_per_genome / mean_fam_per_genome,
        "shell_fraction": p.mean_shell_families_count_per_genome / mean_fam_per_genome,
        "cloud_fraction": p.mean_cloud_families_count_per_genome / mean_fam_per_genome,
    }


def _normalize_genome_category(category: str | None) -> str:
    if not category:
        return "Unknown"

    normalized = category.strip().lower()
    if normalized == "isolate":
        return "Isolate"
    if normalized in {"mag", "mags"}:
        return "MAGs"
    if normalized in {"sag", "sags"}:
        return "SAGs"
    return "Unknown"


def get_pangenome_genome_category_counts(
    session: Session, pangenome_id: int
) -> dict[str, int]:
    query = (
        select(cast(Any, Genome.genome_category), func.count(cast(Any, Genome.id)))
        .join(
            GenomePangenomeLink,
            cast(Any, Genome.id) == cast(Any, GenomePangenomeLink.genome_id),
        )
        .where(cast(Any, GenomePangenomeLink.pangenome_id) == pangenome_id)
        .group_by(cast(Any, Genome.genome_category))
    )

    counts_by_category: defaultdict[str, int] = defaultdict(int)
    for category, count in session.exec(query).all():
        counts_by_category[_normalize_genome_category(category)] += count

    return dict(counts_by_category)


def get_pangenome_genome_category_counts_by_pangenome(
    session: Session, pangenome_ids: Sequence[int]
) -> dict[int, dict[str, int]]:
    if not pangenome_ids:
        return {}

    query = (
        select(
            cast(Any, GenomePangenomeLink.pangenome_id),
            cast(Any, Genome.genome_category),
            func.count(cast(Any, Genome.id)),
        )
        .join(
            Genome,
            cast(Any, Genome.id) == cast(Any, GenomePangenomeLink.genome_id),
        )
        .where(cast(Any, GenomePangenomeLink.pangenome_id).in_(pangenome_ids))
        .group_by(
            cast(Any, GenomePangenomeLink.pangenome_id),
            cast(Any, Genome.genome_category),
        )
    )

    counts_by_pangenome: defaultdict[int, defaultdict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    for pangenome_id, category, count in session.exec(query).all():
        counts_by_pangenome[pangenome_id][_normalize_genome_category(category)] += count

    return {
        pangenome_id: dict(category_counts)
        for pangenome_id, category_counts in counts_by_pangenome.items()
    }


def make_pangenome_public(
    session: Session,
    pangenome: Pangenome,
    genome_category_counts: dict[str, int] | None = None,
) -> PangenomePublic:

    assert pangenome.id is not None, "Pangenome id should not be None"

    taxonomies = get_taxonomies_from_taxa(pangenome.taxa)

    assert (
        len(taxonomies) == 1
    ), f"{pangenome.file_name} {pangenome.id} have an issue with its taxonomies. Found {len(taxonomies)} taxonomies"

    collection_release_public = CollectionReleasePublic(
        **pangenome.collection_release.model_dump(),
        taxonomy_source=TaxonomySourcePublic(
            **pangenome.collection_release.taxonomy_source.model_dump()
        ),
        collection_name=pangenome.collection_release.collection.name,
        collection=CollectionPublic(
            **pangenome.collection_release.collection.model_dump()
        ),
    )

    pangenome_public = PangenomePublic(
        **pangenome.model_dump(),  # type: ignore
        **make_pangenome_public_metrics(pangenome),
        collection_release=collection_release_public,
        average_families_per_genome=pangenome.mean_persistent_families_count_per_genome
        + pangenome.mean_shell_families_count_per_genome
        + pangenome.mean_cloud_families_count_per_genome,
        taxonomy=TaxonomyPublic(**taxonomies[0].model_dump()),
        genome_category_counts=genome_category_counts
        or get_pangenome_genome_category_counts(session, pangenome.id),
    )

    return pangenome_public


def get_public_pangenome(session: Session, pangenome_id: int) -> PangenomePublic | None:

    pangenome = get_pangenome(session, pangenome_id)
    if pangenome is None:
        return None

    return make_pangenome_public(session, pangenome)


def get_pangenomes(
    session: Session,
    filter_params: FilterGenomeTaxonGenomePangenome | None = None,
    pagination_params: PaginationParams | None = None,
) -> Sequence[Pangenome]:
    query = _build_pangenomes_query(filter_params)

    if pagination_params:
        query = query.offset(pagination_params.offset).limit(pagination_params.limit)

    pangenomes = session.exec(query).all()

    return pangenomes


def get_pangenomes_count(
    session: Session,
    filter_params: FilterGenomeTaxonGenomePangenome | None = None,
) -> int:
    pangenome_ids_query = (
        _build_pangenomes_query(filter_params)
        .with_only_columns(cast(Any, Pangenome.id))
        .order_by(None)
        .subquery()
    )
    count_query = select(func.count()).select_from(pangenome_ids_query)
    return session.exec(count_query).one()


def get_public_pangenomes(
    session: Session,
    filter_params: FilterGenomeTaxonGenomePangenome | None = None,
    pagination_params: PaginationParams | None = None,
) -> Iterator[PangenomePublic]:

    pangenomes = get_pangenomes(session, filter_params, pagination_params)
    pangenome_ids = [
        pangenome.id for pangenome in pangenomes if pangenome.id is not None
    ]
    counts_by_pangenome = get_pangenome_genome_category_counts_by_pangenome(
        session, pangenome_ids
    )

    public_pangenomes = (
        make_pangenome_public(
            session,
            pangenome,
            genome_category_counts=(
                counts_by_pangenome.get(pangenome.id, {})
                if pangenome.id is not None
                else {}
            ),
        )
        for pangenome in pangenomes
    )

    return public_pangenomes


def get_genomes_in_pangenome(
    session: Session,
    pangenome_id: int,
    filter_genome: FilterGenome | None = None,
    filter_metadata: FilterGenomeMetadata | None = None,
    pagination_params: PaginationParams | None = None,
):
    # Alias for the metadata table
    # metadata_alias = aliased(GenomeInPangenomeMetadata)

    query = (
        select(GenomePangenomeLink)
        .options(selectinload(cast(Any, GenomePangenomeLink.genome)))
        .distinct()
        .join(Pangenome)
        .where(Pangenome.id == pangenome_id)
    )

    if filter_genome and filter_genome.genome_name is not None:
        query = query.join(Genome).where(Genome.name == filter_genome.genome_name)

    if pagination_params:
        query = query.offset(pagination_params.offset).limit(pagination_params.limit)

    # if filter_metadata:
    #     if filter_metadata.metadata_key:
    #         query = query.join(metadata_alias).where(
    #             metadata_alias.key == filter_metadata.metadata_key
    #         )

    #     if filter_metadata.metadata_value is not None:
    #         query = query.where(metadata_alias.value == filter_metadata.metadata_value)

    pangenome_genomes_links = session.exec(query).all()
    return list(pangenome_genomes_links)


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
