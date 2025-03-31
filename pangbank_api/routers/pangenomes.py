from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from pangbank_api.crud import pangenomes as pangenomes_crud
from pangbank_api.crud.common import (
    FilterGenomeTaxonGenomePangenome,
    PaginationParams,
    FilterGenome,
    FilterGenomeMetadata,
)

from pangbank_api.dependencies import SessionDep
from pangbank_api.models import (
    PangenomePublic,
    GenomePangenomeLinkPublic,
    GenomePangenomeLinkWithMetadataPublic,
)
from pangbank_api.config import SettingsDep

router = APIRouter(
    tags=["pangenomes"],
)


@router.get("/pangenomes/", response_model=list[PangenomePublic])
async def get_pangenomes(
    session: SessionDep,
    filter_params: FilterGenomeTaxonGenomePangenome = Depends(),
    pagination_params: PaginationParams = Depends(),
):
    pangenomes = pangenomes_crud.get_public_pangenomes(
        session, filter_params, pagination_params
    )

    return list(pangenomes)


@router.get("/pangenomes/{pangenome_id}", response_model=PangenomePublic)
async def get_pangenome(pangenome_id: int, session: SessionDep):
    pangenome = pangenomes_crud.get_public_pangenome(session, pangenome_id)

    if not pangenome:
        raise HTTPException(status_code=404, detail="Pangenome not found")
    return pangenome


@router.get(
    "/pangenomes/{pangenome_id}/file", response_model=str, response_class=FileResponse
)
async def get_pangenome_file(
    pangenome_id: int, session: SessionDep, settings: SettingsDep
):
    pangenome_file = pangenomes_crud.get_pangenome_file(session, pangenome_id)

    if not pangenome_file:
        raise HTTPException(status_code=404, detail="Pangenome not found")

    pangenome_path = settings.pangbank_data_dir / pangenome_file
    if not pangenome_file.exists():
        raise HTTPException(
            status_code=404, detail=f"Pangenome file {pangenome_file} does not exists"
        )

    return FileResponse(path=pangenome_path.as_posix(), filename="pangenome.h5")


@router.get(
    "/pangenomes/{pangenome_id}/genomes",
    response_model=list[GenomePangenomeLinkPublic],
)
async def get_genomes_in_pangenome(
    pangenome_id: int,
    session: SessionDep,
    filter_genome: FilterGenome = Depends(),
    pagination_params: PaginationParams = Depends(),
):
    pangenome = pangenomes_crud.get_pangenome(session, pangenome_id)
    if not pangenome:
        raise HTTPException(status_code=404, detail="Pangenome not found")

    pangenome_with_genomes_stat = pangenomes_crud.get_genomes_in_pangenome(
        session,
        pangenome_id,
        filter_genome=filter_genome,
        pagination_params=pagination_params,
    )

    return pangenome_with_genomes_stat


@router.get(
    "/pangenomes/{pangenome_id}/genomes_metadata",
    response_model=list[GenomePangenomeLinkWithMetadataPublic],
)
async def get_genomes_metadata_in_pangenome(
    pangenome_id: int,
    session: SessionDep,
    filter_genome: FilterGenome = Depends(),
    filter_metadata: FilterGenomeMetadata = Depends(),
    pagination_params: PaginationParams = Depends(),
):

    pangenome = pangenomes_crud.get_pangenome(session, pangenome_id)
    if not pangenome:
        raise HTTPException(status_code=404, detail="Pangenome not found")

    pangenome_with_genomes_stat = pangenomes_crud.get_genomes_in_pangenome(
        session,
        pangenome_id,
        filter_genome=filter_genome,
        filter_metadata=filter_metadata,
        pagination_params=pagination_params,
    )

    return pangenome_with_genomes_stat


@router.get(
    "/pangenomes/{pangenome_id}/{genome_id}",
    response_model=GenomePangenomeLinkPublic,
)
async def get_genome_in_pangenome(
    pangenome_id: int, genome_id: int, session: SessionDep
):

    pangenome = pangenomes_crud.get_pangenome(session, pangenome_id)
    if not pangenome:
        raise HTTPException(status_code=404, detail="Pangenome not found")

    pangenome_with_genomes_stat = pangenomes_crud.get_genome_in_pangenome(
        session, pangenome_id, genome_id
    )
    if not pangenome_with_genomes_stat:
        raise HTTPException(
            status_code=404,
            detail=f"Genome id={genome_id} not linked with pangenome id={pangenome_id}",
        )
    return pangenome_with_genomes_stat


@router.get("/pangenomes/count/", response_model=int)
async def get_pangenome_count(
    session: SessionDep, filter_params: FilterGenomeTaxonGenomePangenome = Depends()
):
    pangenomes = pangenomes_crud.get_pangenomes(session, filter_params)

    return len(pangenomes)
