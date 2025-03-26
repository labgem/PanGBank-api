from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from pangbank_api.crud import collections as collections_crud
from pangbank_api.crud.common import FilterCollection, FilterRelease

from ..dependencies import SessionDep
from ..models import CollectionPublicWithReleases

from ..config import SettingsDep

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
async def get_collection_mash_sketch(
    collection_id: int, session: SessionDep, settings: SettingsDep
):

    mash_sketch_file = collections_crud.get_collection_mash_sketch(
        session, collection_id
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
