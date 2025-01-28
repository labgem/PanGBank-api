
from typing import Iterator, Sequence
from sqlmodel import Session, select
from sqlalchemy import func


from app.models import Pangenome, PangenomePublic, Genome, CollectionRelease, GenomePangenomeLink, PangenomeTaxonLink, Taxon

from pathlib import Path

from app.crud.common import FilterPangenome, get_taxonomies_from_taxa, PaginationParams



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

    return get_public_pangenome(pangenome)


def get_public_pangenome(pangenome:Pangenome) -> PangenomePublic:

    taxonomies = get_taxonomies_from_taxa(pangenome.taxa)

    assert len(taxonomies) == 1 

    pangenome_public = PangenomePublic.model_validate(pangenome, from_attributes=True, update={"taxonomy":taxonomies[0]})

    return pangenome_public


def get_pangenomes(session:Session, filter_params: FilterPangenome, pagination_params: PaginationParams | None = None) -> Sequence[Pangenome]:

    query = select(Pangenome)
    
    if filter_params.collection_release_id is not None:
        query = query.where(Pangenome.collection_release_id == filter_params.collection_release_id)

    # if filter_params.collection_id is not None:
    #     query = query.join(CollectionRelease).where(
    #         CollectionRelease.collection_id == filter_params.collection_id
    #     )

    if filter_params.genome_name is not None:
        query = query.join(
                GenomePangenomeLink).join(
                Genome).where(
                    Genome.name == filter_params.genome_name)
        
    if filter_params.taxon_name is not None:
        # Apply offset and limit
        
        query = query.join(
                        PangenomeTaxonLink).join(
                        Taxon)
        if filter_params.substring_match:
            query = query.where(func.lower(Taxon.name).like(f"%{filter_params.taxon_name.lower()}%"))
        else:
            # exact match
            query = query.where(
                            Taxon.name == filter_params.taxon_name)
    # Apply offset and limit
    if pagination_params:
        query = query.offset(pagination_params.offset).limit(pagination_params.limit)

    pangenomes = session.exec(query).all()

    return pangenomes



def get_public_pangenomes(session:Session, filter_params: FilterPangenome, pagination_params: PaginationParams | None = None) -> Iterator[PangenomePublic]:


    pangenomes = get_pangenomes(session, filter_params, pagination_params)
                   
    public_pangenomes = (get_public_pangenome(pangenome) for pangenome in pangenomes)
    
    return public_pangenomes 