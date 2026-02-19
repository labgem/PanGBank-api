from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from pangbank_api.crud import pangenomes as pangenomes_crud
from pangbank_api.crud.common import (
    FilterGenomeTaxonGenomePangenome,
    PaginationParams,
    FilterGenome,
)

from pangbank_api.dependencies import SessionDep
from pangbank_api.models import PangenomePublic, GenomePangenomeLinkPublic
from pangbank_api.config import SettingsDep
from pathlib import Path

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
    pangenome = pangenomes_crud.get_pangenome(session, pangenome_id)

    if not pangenome:
        raise HTTPException(status_code=404, detail="Pangenome not found")

    pangenome_relative_path = (
        Path(pangenome.collection_release.pangenomes_directory) / pangenome.file_name
    )

    pangenome_full_path = settings.pangbank_data_dir / pangenome_relative_path
    if not pangenome_full_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Pangenome file {pangenome_relative_path} does not exists",
        )

    return FileResponse(
        path=pangenome_full_path.as_posix(),
        filename=f"{pangenome.name}_id{pangenome.id}.h5",
    )


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


@router.get(
    "/pangenomes/{pangenome_id}/{genome_id}/cgview_map",
    response_model=str,
    response_class=FileResponse,
)
async def get_genome_cgview_map(
    pangenome_id: int, genome_id: int, session: SessionDep, settings: SettingsDep
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

    cgview_map_relative_path = (
        Path(pangenome.collection_release.pangenomes_directory)
        / pangenome.name
        / "proksee"
        / f"{pangenome_with_genomes_stat.genome.name}.json.gz"
    )

    cgview_map_full_path = settings.pangbank_data_dir / cgview_map_relative_path
    # cgview_map_relative_path

    if not cgview_map_full_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"cgview map '{cgview_map_relative_path}' does not exist",
        )

    return FileResponse(
        cgview_map_full_path,
        filename=f"{pangenome.name}_id{pangenome.id}_{pangenome_with_genomes_stat.genome.name}.json.gz",
        media_type="application/json",
        headers={"Content-Encoding": "gzip", "Content-Type": "application/json"},
    )


@router.get("/pangenomes/count/", response_model=int)
async def get_pangenome_count(
    session: SessionDep, filter_params: FilterGenomeTaxonGenomePangenome = Depends()
):
    pangenomes = pangenomes_crud.get_pangenomes(session, filter_params)

    return len(pangenomes)

@router.get(
    "/pangenomes/{pangenome_id}/dbg/graph", response_model=str, response_class=FileResponse,
    include_in_schema=False
)
async def get_pangenome_dbg(
    pangenome_id: int, session: SessionDep, settings: SettingsDep
):
    pangenome = pangenomes_crud.get_pangenome(session, pangenome_id)

    if not pangenome:
        raise HTTPException(status_code=404, detail="Pangenome not found")

    dbg_relative_path = (
        Path(pangenome.collection_release.pangenomes_directory) / f"../metapang/pangenome_dbg/{pangenome.name}/{pangenome.name}.dbg"
    )
    dbg_full_path = settings.pangbank_data_dir / dbg_relative_path

    if not dbg_full_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"DBG file {dbg_relative_path} does not exists",
        )
    return FileResponse(
        path=dbg_full_path.as_posix(),
        filename=f"{pangenome.name}_id{pangenome.id}.dbg",
    )


def get_annotation(pangenome_id: int, suffix: str, session: SessionDep, settings: SettingsDep) -> FileResponse:
    pangenome = pangenomes_crud.get_pangenome(session, pangenome_id)

    if not pangenome:
        raise HTTPException(status_code=404, detail="Pangenome not found")

    dbg_annotations_relative_path = (
        Path(pangenome.collection_release.pangenomes_directory) / f"../metapang/pangenome_dbg/{pangenome.name}/{pangenome.name}{suffix}.row_diff_brwt.annodbg"
    )
    dbg_annotations_full_path = settings.pangbank_data_dir / dbg_annotations_relative_path

    if not dbg_annotations_full_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"DBG annotations file {dbg_annotations_relative_path} does not exists",
        )

    return FileResponse(
        path=dbg_annotations_full_path.as_posix(),
        filename=f"{pangenome.name}_id{pangenome.id}{suffix}.row_diff_brwt.annodbg",
    )

@router.get(
    "/pangenomes/{pangenome_id}/dbg/family_annotations", response_model=str, response_class=FileResponse,
    include_in_schema=False
)
async def get_pangenome_dbg_annotations(
    pangenome_id: int, session: SessionDep, settings: SettingsDep
):
    return get_annotation(pangenome_id, "", session, settings)

@router.get(
    "/pangenomes/{pangenome_id}/dbg/genome_annotations", response_model=str, response_class=FileResponse,
    include_in_schema=False
)
async def get_pangenome_dbg_annotations_genomes(
    pangenome_id: int, session: SessionDep, settings: SettingsDep
):
    return get_annotation(pangenome_id, "_genomes", session, settings)

