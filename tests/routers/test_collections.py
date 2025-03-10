from datetime import datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient
from httpx import Response
from sqlmodel import Session


from app.models import Collection, CollectionRelease, TaxonomySource
from tests.mock_session import session_fixture, client_fixture  # type: ignore # noqa: F401 # pylint: disable=unused-import


@pytest.fixture
def collection_release_data() -> dict[str, Any]:
    return {
        "version": "1.0.0",
        "ppanggolin_version": "2.3.4",
        "pangbank_wf_version": "1.2.3",
        "pangenomes_directory": "/path/to/pangenomes",
        "release_note": "Initial release.",
        "mash_sketch": "sketch/path",
        "mash_version": "2.0",
        "date": datetime.now(),
        "collection_id": 1,
        "taxonomy_source_id": 1,
    }


@pytest.fixture(name="release")
def create_collection_release(
    session: Session, collection_release_data: dict[str, Any]
) -> CollectionRelease:
    release = CollectionRelease(**collection_release_data)
    session.add(release)
    session.commit()
    session.refresh(release)
    return release


def test_get_collections(
    session: Session, client: TestClient, collection_release_data: dict[str, Any]
):
    taxonomy_source = TaxonomySource(
        name="Taxonomy Source 1",
        ranks="Domain; Phylum; Class; Order; Family; Genus; Species",
    )
    collection_1 = Collection(name="Collection 1")
    collection_2 = Collection(name="Collection 2")

    release1 = CollectionRelease(
        **collection_release_data,
        collection=collection_1,
        taxonomy_source=taxonomy_source,
    )

    release2 = CollectionRelease(
        **collection_release_data,
        collection=collection_2,
        taxonomy_source=taxonomy_source,
    )

    session.add(release1)
    session.add(release2)
    session.commit()
    session.refresh(release1)
    session.refresh(release2)

    response: Response = client.get("/collections/")

    assert response.status_code == 200

    data = response.json()

    assert len(data) == 2

    assert data[0]["name"] == collection_1.name
    assert data[1]["name"] == collection_2.name

    assert len(data[0]["releases"]) == 1
    assert len(data[1]["releases"]) == 1

    assert data[0]["releases"][0]["version"] == release1.version
    assert data[1]["releases"][0]["version"] == release2.version

    assert (
        data[0]["releases"][0]["pangenomes_directory"] == release1.pangenomes_directory
    )
    assert (
        data[1]["releases"][0]["pangenomes_directory"] == release2.pangenomes_directory
    )


def test_get_collection(session: Session, client: TestClient):

    collection_1 = Collection(name="Collection 1")

    session.add(collection_1)
    session.commit()
    session.refresh(collection_1)

    collection_id = collection_1.id

    response: Response = client.get(f"/collections/{collection_id}")

    assert response.status_code == 200

    data = response.json()
    assert data["name"] == collection_1.name
