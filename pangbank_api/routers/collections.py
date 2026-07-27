from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse

from pangbank_api.crud import collections as collections_crud
from pangbank_api.crud.common import (
    FilterCollection,
    FilterRelease,
    FilterReleaseVersion,
)

from ..dependencies import SessionDep, SettingsDep
from ..models import CollectionPublicWithReleases, MetaPanGCollection

import json

router = APIRouter(
    tags=["collections"],
    responses={404: {"description": "Not found"}},
)


@router.get("/collections/", response_model=list[CollectionPublicWithReleases])
def get_collections(session: SessionDep, filter_params: FilterCollection = Depends()):
    collections = collections_crud.get_collections(session, filter_params)

    return collections


@router.get("/collections/{collection_id}", response_model=CollectionPublicWithReleases)
def get_collection(
    collection_id: int, session: SessionDep, filter_release: FilterRelease = Depends()
):

    collection = collections_crud.get_collection(session, collection_id, filter_release)

    if not collection:
        raise HTTPException(
            status_code=404, detail=f"Collection with id={collection_id} not found"
        )

    return collection


@router.get(
    "/collections/{collection_id}/mash_sketch",
    response_model=str,
    response_class=FileResponse,
)
async def get_collection_mash_release_sketch(
    collection_id: int,
    session: SessionDep,
    settings: SettingsDep,
    filter_release: FilterReleaseVersion = Depends(),
):

    mash_sketch_file = collections_crud.get_collection_release_mash_sketch(
        session, collection_id, filter_release
    )
    if not mash_sketch_file:
        raise HTTPException(
            status_code=404,
            detail=f"Mash sketch of collection with id={collection_id} not found",
        )
    mash_sketch_path = settings.pangbank_data_dir / mash_sketch_file

    if not mash_sketch_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Pangenome file {mash_sketch_file.name} does not exists",
        )
    return FileResponse(path=mash_sketch_path.as_posix(), filename="mash_sketch.msh")


@router.get(
    "/collections/{collection_id}/multiqc_report",
    response_class=HTMLResponse,
)
async def get_collection_release_multiqc(
    collection_id: int,
    session: SessionDep,
    settings: SettingsDep,
    filter_release: FilterReleaseVersion = Depends(),
):

    multiqc_file = collections_crud.get_collection_release_multiqc(
        session, collection_id, filter_release
    )
    if not multiqc_file:
        raise HTTPException(
            status_code=404,
            detail=f"MultiQC report of collection with id={collection_id} not found",
        )
    multiqc_path = settings.pangbank_data_dir / multiqc_file

    if not multiqc_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"MultiQC file {multiqc_file.name} does not exists",
        )
    return HTMLResponse(content=multiqc_path.read_text(), media_type="text/html")


@router.get(
    "/collections/{collection_id}/release_notes",
    response_class=PlainTextResponse,
)
async def get_collection_release_notes(
    collection_id: int,
    session: SessionDep,
    settings: SettingsDep,
    filter_release: FilterReleaseVersion = Depends(),
):

    release_notes_file = collections_crud.get_collection_release_notes(
        session, collection_id, filter_release
    )
    if not release_notes_file:
        raise HTTPException(
            status_code=404,
            detail=f"Release notes of collection with id={collection_id} not found",
        )
    release_notes_path = settings.pangbank_data_dir / release_notes_file

    if not release_notes_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Release notes file {release_notes_file.name} does not exists",
        )
    return release_notes_path.read_text()


@router.get(
    "/collections/{collection_id}/index/info",
    response_model=str,
    response_class=FileResponse,
    include_in_schema=False
)
async def get_collection_index_info(
    collection_id: int, session: SessionDep, settings: SettingsDep
):
    index_directory = collections_crud.get_collection_index_directory(session, collection_id)

    if not index_directory:
        raise HTTPException(
            status_code=404,
            detail=f"Index info of collection with id={collection_id} not found",
        )

    index_info_file = settings.pangbank_data_dir / index_directory / "index_info.json"

    if not index_info_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Index info file {index_info_file.name} does not exists",
        )

    return FileResponse(path=index_info_file, filename="index_info.json")

@router.get(
    "/collections/metapang",
    response_model=list[MetaPanGCollection],
    include_in_schema=False
)
async def get_metapang_ready_collections(session: SessionDep, settings: SettingsDep):
    metapang_file = settings.pangbank_data_dir / "metapang.json"

    if not metapang_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"MetaPanG data are missing."
        )

    with open(metapang_file) as mf:
        metapang_data = json.load(mf)

    try:
        return [MetaPanGCollection(**entry) for entry in metapang_data]
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Internal error"
        )

@router.get(
    "/collections/{collection_id}/index/pangenomes",
    response_model=str,
    response_class=FileResponse,
    include_in_schema=False
)
async def get_collection_index_pangenomes(
    collection_id: int, session: SessionDep, settings: SettingsDep
):
    index_directory = collections_crud.get_collection_index_directory(session, collection_id)

    if not index_directory:
        raise HTTPException(
            status_code=404,
            detail=f"Index pangenomes of collection with id={collection_id} not found",
        )

    index_pangenome_file = settings.pangbank_data_dir / index_directory / "pangenome_index.sbt.zip"

    if not index_pangenome_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Index pangenome file {index_pangenome_file.name} does not exists",
        )

    return FileResponse(path=index_pangenome_file, filename="pangenome_index.sbt.zip")

@router.get(
    "/collections/{collection_id}/index/genomes",
    response_model=str,
    response_class=FileResponse,
    include_in_schema=False
)
async def get_collection_index_genomes(
    collection_id: int, session: SessionDep, settings: SettingsDep
):
    index_directory = collections_crud.get_collection_index_directory(session, collection_id)

    if not index_directory:
        raise HTTPException(
            status_code=404,
            detail=f"Index genomes of collection with id={collection_id} not found",
        )

    index_genome_file = settings.pangbank_data_dir / index_directory / "genome_index.sbt.zip"

    if not index_genome_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Index genome file {index_genome_file.name} does not exists",
        )

    return FileResponse(path=index_genome_file, filename="genome_index.sbt.zip")
