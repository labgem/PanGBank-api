from typing import List, Sequence

from sqlmodel import Session, select

from packaging.version import parse

from app.crud.common import FilterCollection
from app.models import (
    Collection,
    CollectionRelease,
    CollectionReleasePublicWithCount,
    CollectionPublicWithReleases,
)


def get_collections(
    session: Session, filter_params: FilterCollection
) -> Sequence[CollectionPublicWithReleases]:

    query = select(Collection)

    # Check if filter_params.collection_release_id is provided
    if filter_params.collection_release_id is not None:

        query = query.join(CollectionRelease).where(
            CollectionRelease.id == filter_params.collection_release_id
        )

    collections = session.exec(query).all()

    public_collections: List[CollectionPublicWithReleases] = []
    for collection in collections:

        public_releases: List[CollectionReleasePublicWithCount] = []
        for release in collection.releases:

            release_public = CollectionReleasePublicWithCount.model_validate(
                release,
                from_attributes=True,
                update={"pangenome_count": len(release.pangenomes)},
            )
            public_releases.append(release_public)

        # Sort using semantic versioning
        public_releases = sorted(
            public_releases, key=lambda x: parse(x.version), reverse=True
        )

        # Mark the latest release
        if public_releases:
            public_releases[0].latest = True

        collection_public = CollectionPublicWithReleases.model_validate(
            collection,
            from_attributes=True,
            update={"releases": public_releases},
        )

        public_collections.append(collection_public)

    return public_collections
