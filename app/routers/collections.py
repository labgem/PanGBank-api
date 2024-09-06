from fastapi import APIRouter, HTTPException, Query, Depends

from ..models import CollectionPublicWithReleases, CollectionPublic, Collection
from ..dependencies import SessionDep
from sqlmodel import select
from app.crud.common import FilterParams
from app.crud import collections as collections_crud


router = APIRouter(
    tags=["collections"],
    responses={404: {"description": "Not found"}},
)


@router.get("/collections/", response_model=list[CollectionPublicWithReleases])
def get_collections(session: SessionDep, filter_params: FilterParams = Depends() ): # offset: int = 0, limit: int = Query(default=100, le=100)

    
    collections = collections_crud.get_collections(session, filter_params)

    
    return collections

@router.get("/collections/{collection_id}", response_model=CollectionPublicWithReleases)
def get_collection(collection_id:int, session: SessionDep):
    
       
    collection = session.get(Collection, collection_id)

    if not collection:
        raise HTTPException(status_code=404, detail=f"Collection with id={collection_id} not found")
    
    return collection

