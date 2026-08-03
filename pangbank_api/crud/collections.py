from typing import List, Sequence

from pathlib import Path

from sqlmodel import Session, select
from packaging.version import parse

from pangbank_api.crud.common import (
    FilterCollectionAndRelease,
    FilterRelease,
    FilterReleaseVersion,
)
from pangbank_api.models import (
    Collection,
    CollectionReleasePublic,
    CollectionPublicWithReleases,
    CollectionRelease,
)

from sqlalchemy.orm import selectinload, contains_eager

def get_collections(
    session: Session, filter_params: FilterCollectionAndRelease
) -> Sequence[CollectionPublicWithReleases]:

    query = select(Collection)

    if filter_params.collection_id is not None:
        query = query.where(Collection.id == filter_params.collection_id)

    if filter_params.collection_name is not None:
        query = query.where(Collection.name == filter_params.collection_name)

    if filter_params.release_version is not None or filter_params.only_latest_release:
        query = (
            query.join(Collection.releases)  # pyright: ignore
            .where(
                *(
                    [CollectionRelease.version == filter_params.release_version]
                    if filter_params.release_version is not None
                    else []
                ),
                *(
                    [CollectionRelease.latest.is_(True)]  # type: ignore
                    if filter_params.only_latest_release
                    else []
                ),
            )
            .options(contains_eager(Collection.releases))  # pyright: ignore
        )

    else:
        query = query.options(selectinload(Collection.releases))  # pyright: ignore

    collections = session.exec(query).unique().all()

    return [
        make_collection_public_with_releases(collection) for collection in collections
    ]


def make_collection_public_with_releases(
    collection: Collection,
    only_latest_release: bool | None = None,
    version: str | None = None,
):
    public_releases: List[CollectionReleasePublic] = []

    if version is not None:
        releases = [r for r in collection.releases if r.version == version]
        if not releases:
            raise ValueError(
                f"No release found with version '{version}' "
                f"in collection '{collection.name}'"
            )

    elif only_latest_release:
        latest_release = max(collection.releases, key=lambda x: parse(x.version))

        assert (
            latest_release.latest
        ), f"Latest release {latest_release} is not marked as latest"

        releases = [latest_release]
    else:
        releases = collection.releases

    for release in releases:
        release_public = CollectionReleasePublic.model_validate(
            release,
            from_attributes=True,
            update={
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


def get_collection(
    session: Session,
    collection_id: int,
    filter_release: FilterRelease,
):
    query = select(Collection).where(Collection.id == collection_id)

    if filter_release.release_version is not None or filter_release.only_latest_release:
        conditions = []

        if filter_release.release_version is not None:
            conditions.append(
                CollectionRelease.version == filter_release.release_version
            )

        if filter_release.only_latest_release:
            conditions.append(CollectionRelease.latest.is_(True))

        query = (
            query.join(Collection.releases)
            .where(*conditions)
            .options(contains_eager(Collection.releases))
        )

    else:
        query = query.options(selectinload(Collection.releases))

    collection = session.exec(query).unique().one_or_none()

    if collection is None:
        return None

    return make_collection_public_with_releases(collection)


def get_collection_release(
    session: Session, collection_id: int, filter_release: FilterReleaseVersion
):

    collection = session.get(Collection, collection_id)

    if not collection:
        return None

    if filter_release.release_version is not None:
        releases = [
            r
            for r in collection.releases
            if r.version == filter_release.release_version
        ]
        if not releases:
            raise ValueError(
                f"No release found with version '{filter_release.release_version}' "
                f"in collection '{collection.name}'"
            )
        release = releases[0]
    else:
        release = max(collection.releases, key=lambda x: parse(x.version))

    return release


def get_collection_release_mash_sketch(
    session: Session, collection_id: int, filter_release: FilterReleaseVersion
):
    release = get_collection_release(session, collection_id, filter_release)

    if release is None:
        return None

    return Path(release.mash_sketch)


def get_collection_release_multiqc(
    session: Session, collection_id: int, filter_release: FilterReleaseVersion
):
    release = get_collection_release(session, collection_id, filter_release)

    if release is None:
        return None

    return (
        Path(release.pangenomes_directory) / ".." / "multiqc/multiqc_report_public.html"
    )


def get_collection_release_notes(
    session: Session, collection_id: int, filter_release: FilterReleaseVersion
):
    release = get_collection_release(session, collection_id, filter_release)

    if release is None:
        return None

    return Path(release.pangenomes_directory) / ".." / "release_notes.md"


def get_collection_index_directory(session: Session, collection_id: int):

    collection = session.get(Collection, collection_id)

    if not collection:
        return None

    latest_release = max(collection.releases, key=lambda x: parse(x.version))

    return Path(latest_release.pangenomes_directory) / ".." / "metapang/bank_index"
