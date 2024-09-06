from fastapi import APIRouter, HTTPException, Query

from ..models import CollectionPublicWithReleases, CollectionPublic, Collection
from ..dependencies import SessionDep
from sqlmodel import select

router = APIRouter(
    tags=["collections"],
    responses={404: {"description": "Not found"}},
)


@router.get("/collections/", response_model=list[CollectionPublicWithReleases])
def read_collections(session: SessionDep, offset: int = 0, limit: int = Query(default=100, le=100)):

    collections = session.exec(select(Collection).offset(offset).limit(limit)).all()
    
    return collections

@router.get("/collections/{collection_id}", response_model=CollectionPublicWithReleases)
def get_collection_by_name(collection_id:int, session: SessionDep):
    
       
    collection = session.get(Collection, collection_id)

    if not collection:
        raise HTTPException(status_code=404, detail=f"Collection with name {collection_name} not found")
    
    return collection

