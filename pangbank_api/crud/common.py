from fastapi import Query
from pydantic import BaseModel

from pangbank_api.models import Taxon, Taxonomy
from typing import Annotated

class PaginationParams(BaseModel):
    offset: int = 0
    limit: int = Query(default=20, le=100)


class FilterRelease(BaseModel):
    only_latest_release: bool | None = None


class FilterCollection(FilterRelease):
    collection_name: str | None = None
    collection_id: int | None = None


class FilterTaxon(BaseModel):
    taxon_name: Annotated[str | None, Query(min_length=3)] = None
    substring_match: bool = False


class FilterGenome(BaseModel):
    genome_name: str | None = None


class FilterGenomeMetadata(BaseModel):
    metadata_key: str | None = None
    metadata_value: str | None = None
    substring_match: bool = False


class FilterGenomeTaxon(FilterGenome, FilterTaxon):
    genome_name: str | None = None


class FilterCollectionTaxonGenome(
    FilterCollection,
    FilterTaxon,
    FilterGenome,
):
    pass


def get_taxonomies_from_taxa(taxa: list[Taxon]) -> list[Taxonomy]:
    taxonomy_source_to_taxonomy: dict[int, Taxonomy] = {}

    for taxon in taxa:
        if taxon.taxonomy_source_id in taxonomy_source_to_taxonomy:
            taxonomy = taxonomy_source_to_taxonomy[taxon.taxonomy_source_id]
            taxonomy.taxa.append(taxon)
        else:
            taxonomy = Taxonomy(taxa=[taxon], taxonomy_source=taxon.taxonomy_source)

            if taxon.taxonomy_source_id is not None:
                taxonomy_source_to_taxonomy[taxon.taxonomy_source_id] = taxonomy
            else:
                raise ValueError(
                    f"Taxon {taxon} has no taxonomy source associated with it"
                )
    taxonomies = list(taxonomy_source_to_taxonomy.values())
    return taxonomies
