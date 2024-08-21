from fastapi import APIRouter, HTTPException, Query

from ..models import GenomePublic, Genome
from ..dependencies import SessionDep
from sqlmodel import select

router = APIRouter(
    tags=["genomes"],
    responses={404: {"description": "Not found"}},
)

@router.get("/genomes/", response_model=list[GenomePublic])
def read_genomes(session: SessionDep, offset: int = 0, limit: int = Query(default=100, le=100)):
    genomes = session.exec(select(Genome).offset(offset).limit(limit)).all()
    # genomes = session.exec(select(Genome)).all()

    return genomes

@router.get("/genomes/{genome_id}", response_model=GenomePublic)
def get_genome(genome_id:int, session: SessionDep):

    genome = session.get(Genome, genome_id)
    if not genome:
        raise HTTPException(status_code=404, detail="Genome not found")
    return genome
