
from sqlmodel import Session, select

from app.routers import genomes
from app.models import GenomePublic, Pangenome, PangenomePublic, Genome, Taxon, Taxonomy, GenomePublicWithTaxonomies

from pathlib import Path

from app.crud.common import get_taxonomies_from_taxa


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
