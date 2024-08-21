from fastapi import APIRouter, HTTPException

from ..models import CollectionPublicWithReleases, CollectionPublic, Collection
from ..dependencies import SessionDep
from sqlmodel import select

router = APIRouter(
    tags=["collections"],
    responses={404: {"description": "Not found"}},
)


@router.get("/collections/", response_model=list[CollectionPublic])
def read_collections(session: SessionDep):

    collections = session.exec(select(Collection)).all()
    return collections

@router.get("/collections/{collection_id}", response_model=CollectionPublicWithReleases)
def get_collection(collection_id, session: SessionDep):

    collection = session.get(Collection, collection_id)
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")
    
    return collection

