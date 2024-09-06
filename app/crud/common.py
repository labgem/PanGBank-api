
from sqlmodel import Session, select
from pydantic import BaseModel
from pathlib import Path
from fastapi import Query


from app.models import GenomePublic, Pangenome, PangenomePublic, Genome, Taxon, Taxonomy, GenomePublicWithTaxonomies



class FilterParams(BaseModel):

    offset: int = 0
    limit: int = Query(default=20, le=100)

    collection_release_id: int | None = None
    # Add more filters as needed

class FilterGenome(FilterParams):

    offset: int = 0
    limit: int = Query(default=20, le=100)

    collection_release_id: int | None = None
    collection_id: int | None = None

    genome_name : str | None = None
    genome_id : int | None = None
    
    taxon_name : str | None = None
    

def get_taxonomies_from_taxa(taxa: list[Taxon]) -> list[Taxonomy]:

    taxonomy_source_to_taxonomy = {}
    for taxon in taxa:
        if taxon.taxonomy_source_id in taxonomy_source_to_taxonomy:
            taxonomy = taxonomy_source_to_taxonomy[taxon.taxonomy_source_id] 
            taxonomy.taxa.append(taxon)
        else:
            taxonomy = Taxonomy(taxa = [taxon], taxonomy_source=taxon.taxonomy_source)
            taxonomy_source_to_taxonomy[taxon.taxonomy_source_id]  = taxonomy

    taxonomies = list(taxonomy_source_to_taxonomy.values())
    return taxonomies



