from fastapi import APIRouter, Depends, HTTPException

from pangbank_api.crud import collections as collections_crud
from pangbank_api.crud.common import FilterCollection

from ..dependencies import SessionDep
from ..models import Collection, CollectionPublicWithReleases

router = APIRouter(
    tags=["collections"],
    responses={404: {"description": "Not found"}},
)


@router.get("/collections/", response_model=list[CollectionPublicWithReleases])
def get_collections(
    session: SessionDep, filter_params: FilterCollection = Depends()
):  # offset: int = 0, limit: int = Query(default=100, le=100)

    collections = collections_crud.get_collections(session, filter_params)

    return collections


@router.get("/collections/{collection_id}", response_model=CollectionPublicWithReleases)
def get_collection(collection_id: int, session: SessionDep):

    collection = session.get(Collection, collection_id)

    if not collection:
        raise HTTPException(
            status_code=404, detail=f"Collection with id={collection_id} not found"
        )

    return collection
