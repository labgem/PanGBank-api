from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from pangbank_api.crud import pangenomes as pangenomes_crud
from pangbank_api.crud.common import (
    FilterCollectionTaxonGenome,
    PaginationParams,
    FilterGenome,
)

from ..dependencies import SessionDep
from ..models import PangenomePublic, GenomePangenomeLinkPublic

router = APIRouter(
    tags=["pangenomes"],
)


@router.get("/pangenomes/", response_model=list[PangenomePublic])
async def get_pangenomes(
    session: SessionDep,
    filter_params: FilterCollectionTaxonGenome = Depends(),
    pagination_params: PaginationParams = Depends(),
):
    pangenomes = pangenomes_crud.get_public_pangenomes(
        session, filter_params, pagination_params
    )

    return list(pangenomes)


@router.get("/pangenomes/{pangenome_id}", response_model=PangenomePublic)
async def get_pangenome(pangenome_id: int, session: SessionDep):
    pangenome = pangenomes_crud.get_pangenome(session, pangenome_id)

    if not pangenome:
        raise HTTPException(status_code=404, detail="Pangenome not found")
    return pangenome


@router.get(
    "/pangenomes/{pangenome_id}/file", response_model=str, response_class=FileResponse
)
async def get_pangenome_file(pangenome_id: int, session: SessionDep):
    pangenome_file = pangenomes_crud.get_pangenome_file(session, pangenome_id)

    if not pangenome_file:
        raise HTTPException(status_code=404, detail="Pangenome not found")

    if not pangenome_file.exists():
        raise HTTPException(
            status_code=404, detail=f"Pangenome file {pangenome_file} does not exists"
        )

    return FileResponse(path=pangenome_file.as_posix(), filename="pangenome.h5")


@router.get(
    "/pangenomes/{pangenome_id}/genomes",
    response_model=list[GenomePangenomeLinkPublic],
)
async def getègenomes_in_pangenomes(
    pangenome_id: int,
    session: SessionDep,
    filter_params: FilterGenome = Depends(),
    pagination_params: PaginationParams = Depends(),
):
    pangenome_with_genomes_stat = pangenomes_crud.get_pangenome_with_genomes_info(
        session,
        pangenome_id,
        filter_params=filter_params,
        pagination_params=pagination_params,
    )

    if not pangenome_with_genomes_stat:
        raise HTTPException(status_code=404, detail="Pangenome not found")
    return pangenome_with_genomes_stat


@router.get("/pangenomes/count/", response_model=int)
async def get_pangenome_count(
    session: SessionDep, filter_params: FilterCollectionTaxonGenome = Depends()
):
    pangenomes = pangenomes_crud.get_pangenomes(session, filter_params)

    return len(pangenomes)
