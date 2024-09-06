
from sqlmodel import Session, select
from app.models import GenomePublic, Pangenome, PangenomePublic, Genome, Taxon, Taxonomy, GenomePublicWithTaxonomies

from pathlib import Path

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



