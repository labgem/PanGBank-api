
from sqlmodel import Session, select
from .models import GenomePublic, Pangenome, PangenomePublic, Genome, Taxon, Taxonomy, GenomePublicWithTaxonomies

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

def get_pangenome_file(session:Session, pangenome_id:int) -> Path | None:

    pangenome = session.get(Pangenome, pangenome_id)

    if not pangenome:
        return None
    
    pangenome_file = Path(pangenome.collection_release.pangenomes_directory) / pangenome.file_name

    return pangenome_file

def get_pangenome(session:Session, pangenome_id:int) -> PangenomePublic | None:

    pangenome = session.get(Pangenome, pangenome_id)
    if pangenome is None:
        return None
    
    taxonomies = get_taxonomies_from_taxa(pangenome.taxa)

    assert len(taxonomies) == 1 

    pangenome_public = PangenomePublic.model_validate(pangenome, from_attributes=True, update={"taxonomy":taxonomies[0],
                                                                                               "genome_count":len(pangenome.genome_links)})

    return pangenome_public


def get_genome_public(genome:Genome) -> GenomePublicWithTaxonomies:

    taxonomies = get_taxonomies_from_taxa(genome.taxa)
    genome_public = GenomePublicWithTaxonomies.model_validate(genome, from_attributes=True, update={"taxonomies":taxonomies})

    return genome_public

def get_genome_by_id(session:Session, genome_id:int) -> GenomePublicWithTaxonomies | None:
    
    genome = session.get(Genome, genome_id)
    if genome is None:
        return None
    
    return get_genome_public(genome)

def get_genome_by_name(session:Session, genome_name:str) -> GenomePublicWithTaxonomies | None:
    
    genome = session.exec(select(Genome).where(Genome.name == genome_name)).first()

    if genome is None:
        return None
    
    return get_genome_public(genome)
