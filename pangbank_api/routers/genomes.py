from fastapi import APIRouter, Depends, HTTPException

from pangbank_api.crud import genomes as genomes_crud
from pangbank_api.crud.common import FilterGenomeTaxon, PaginationParams
from pangbank_api.dependencies import SessionDep
from pangbank_api.models import GenomePublicWithTaxonomies

router = APIRouter(
    tags=["genomes"],
    responses={404: {"description": "Not found"}},
)


@router.get("/genomes/", response_model=list[GenomePublicWithTaxonomies])
async def read_genomes(
    session: SessionDep,
    filter_params: FilterGenomeTaxon = Depends(),
    pagination_params: PaginationParams = Depends(),
):
    genomes = genomes_crud.get_genomes(session, filter_params, pagination_params)

    return genomes


@router.get("/genomes/{genome_id}", response_model=GenomePublicWithTaxonomies)
async def get_genome_by_id(genome_id: int, session: SessionDep):
    # genome = session.get(Genome, genome_id)
    genome = genomes_crud.get_genome_by_id(session=session, genome_id=genome_id)
    if not genome:
        raise HTTPException(status_code=404, detail="Genome not found")
    return genome
