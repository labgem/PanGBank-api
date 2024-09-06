
from sqlmodel import Session, select
from app.models import GenomePublic, Pangenome, PangenomePublic, Genome, Taxon, Taxonomy, GenomePublicWithTaxonomies

from pathlib import Path

from app.crud.common import get_taxonomies_from_taxa


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


def get_pangenomes(session:Session, offset: int, limit: int) -> list[PangenomePublic]:


    pangenomes = session.exec(select(Pangenome).offset(offset).limit(limit)).all()
    public_pangenomes = []
    for pangenome in pangenomes:
        
        taxonomies = get_taxonomies_from_taxa(pangenome.taxa)

        assert len(taxonomies) == 1 

        pangenome_public = PangenomePublic.model_validate(pangenome, from_attributes=True, update={"taxonomy":taxonomies[0],
                                                                                                "genome_count":len(pangenome.genome_links)})
        public_pangenomes.append(pangenome_public)

    return public_pangenomes