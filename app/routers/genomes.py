from fastapi import APIRouter, HTTPException, Query

from ..models import GenomePublic, Genome, GenomePublicWithTaxonomies
from ..dependencies import SessionDep
from ..crud import genomes as genomes_crud
from sqlmodel import select


router = APIRouter(
    tags=["genomes"],
    responses={404: {"description": "Not found"}},
)

@router.get("/genomes/", response_model=list[GenomePublic])
async def read_genomes(session: SessionDep, offset: int = 0, limit: int = Query(default=100, le=100)):

    genomes = session.exec(select(Genome).offset(offset).limit(limit)).all()
    return genomes


@router.get("/genomes/{genome_id}", response_model=GenomePublicWithTaxonomies)
async def get_genome_by_id(genome_id:int, session: SessionDep):

    # genome = session.get(Genome, genome_id)
    genome = genomes_crud.get_genome_by_id(session=session, genome_id=genome_id)
    if not genome:
        raise HTTPException(status_code=404, detail="Genome not found")
    return genome

