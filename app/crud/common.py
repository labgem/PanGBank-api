from pydantic import BaseModel
from fastapi import Query


from app.models import Taxon, Taxonomy


class PaginationParams(BaseModel):

    offset: int = 0
    limit: int = Query(default=20, le=100)


class FilterCollection(BaseModel):
    collection_release_id: int | None = None


class FilterGenome(BaseModel):

    genome_name: str | None = None

    taxon_name: str | None = None

    substring_match: bool = False


class FilterPangenome(FilterGenome, FilterCollection):
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
