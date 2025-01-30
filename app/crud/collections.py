from sqlmodel import Session, select
from typing import Sequence
from app.models import Collection, CollectionRelease

from app.crud.common import FilterCollection


def get_collections(
    session: Session, filter_params: FilterCollection
) -> Sequence[Collection]:

    query = select(Collection)

    # Check if filter_params.collection_release_id is provided
    if filter_params.collection_release_id is not None:

        query = query.join(CollectionRelease).where(
            CollectionRelease.id == filter_params.collection_release_id
        )

    collections = session.exec(query).all()

    return collections
