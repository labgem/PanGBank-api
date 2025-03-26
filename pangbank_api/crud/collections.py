from typing import List, Sequence

from pathlib import Path

from sqlmodel import Session, select
from packaging.version import parse

from pangbank_api.crud.common import FilterCollection, FilterRelease
from pangbank_api.models import (
    Collection,
    CollectionReleasePublicWithCount,
    CollectionPublicWithReleases,
)


def get_collections(
    session: Session, filter_params: FilterCollection
) -> Sequence[CollectionPublicWithReleases]:
    query = select(Collection)

    # Check if filter_params.collection_release_id is provided
    if filter_params.collection_name is not None:
        query = query.where(Collection.name == filter_params.collection_name)

    collections = session.exec(query).all()

    public_collections: List[CollectionPublicWithReleases] = []

    for collection in collections:
        collection_public = make_collection_public_with_releases(
            collection, filter_params.only_latest_release
        )
        public_collections.append(collection_public)

    return public_collections


def make_collection_public_with_releases(
    collection: Collection, only_latest_release: bool | None
):

    public_releases: List[CollectionReleasePublicWithCount] = []

    latest_release = max(collection.releases, key=lambda x: parse(x.version))
    assert (
        latest_release.latest
    ), f"Latest release {latest_release} is not marked as latest"

    if only_latest_release:
        releases = [latest_release]
    else:
        releases = collection.releases

    for release in releases:

        release_public = CollectionReleasePublicWithCount.model_validate(
            release,
            from_attributes=True,
            update={
                "pangenome_count": len(release.pangenomes),
                "collection_name": collection.name,
            },
        )
        public_releases.append(release_public)

    # Sort using semantic versioning
    public_releases = sorted(
        public_releases, key=lambda x: parse(x.version), reverse=True
    )

    collection_public = CollectionPublicWithReleases.model_validate(
        collection,
        from_attributes=True,
        update={
            "releases": public_releases,
        },
    )
    return collection_public


def get_collection(session: Session, collection_id: int, filter_release: FilterRelease):

    collection = session.get(Collection, collection_id)
    if collection is None:
        return None

    public_collection = make_collection_public_with_releases(
        collection, only_latest_release=filter_release.only_latest_release
    )
    return public_collection


def get_collection_mash_sketch(session: Session, collection_id: int):

    collection = session.get(Collection, collection_id)

    if not collection:
        return None

    latest_release = max(collection.releases, key=lambda x: parse(x.version))

    return Path(latest_release.mash_sketch)
