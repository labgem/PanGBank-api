# tests/test_pangenomes.py
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from pangbank_api.models import (
    CollectionRelease,
    Pangenome,
    Taxon,
    TaxonomySource,
    Collection,
)
from tests.mock_session import session_fixture, client_fixture  # type: ignore # noqa: F401 # pylint: disable=unused-import


@pytest.fixture
def collection_release_data(tmp_path: Path) -> dict[str, Any]:
    return {
        "version": "1.0.0",
        "ppanggolin_version": "2.3.4",
        "pangbank_wf_version": "1.2.3",
        "pangenomes_directory": tmp_path.as_posix(),
        "release_note": "Initial release.",
        "mash_sketch": "sketch/path",
        "mash_version": "2.0",
        "date": datetime.now(),
        "collection_id": 1,
        "taxonomy_source_id": 1,
        "mash_sketch_md5sum": "1234567890abcdef",
    }


@pytest.fixture(name="release")
def create_collection_release(
    session: Session, collection_release_data: dict[str, Any]
) -> CollectionRelease:
    release = CollectionRelease(**collection_release_data)
    collection = Collection(
        name="Collection 1",
    )
    release.collection = collection
    session.add(release)
    session.add(collection)
    session.commit()
    session.refresh(release)
    return release


@pytest.fixture
def pangenome_metric_data(session: Session) -> Dict[str, Any]:
    # Sample data as dictionaries
    metric1: Dict[str, Any] = {
        "gene_count": 1000,
        "genome_count": 50,
        "family_count": 200,
        "edge_count": 1500,
        "persistent_family_count": 100,
        "persistent_family_min_genome_frequency": 0.8,
        "persistent_family_max_genome_frequency": 1.0,
        "persistent_family_std_genome_frequency": 0.05,
        "persistent_family_mean_genome_frequency": 0.9,
        "shell_family_count": 50,
        "shell_family_min_genome_frequency": 0.2,
        "shell_family_max_genome_frequency": 0.8,
        "shell_family_std_genome_frequency": 0.15,
        "shell_family_mean_genome_frequency": 0.5,
        "cloud_family_count": 50,
        "cloud_family_min_genome_frequency": 0.0,
        "cloud_family_max_genome_frequency": 0.2,
        "cloud_family_std_genome_frequency": 0.05,
        "cloud_family_mean_genome_frequency": 0.1,
        "partition_count": 10,
        "rgp_count": 5,
        "spot_count": 3,
        "module_count": 15,
        "family_in_module_count": 50,
        "mean_completeness": 0.95,
        "mean_contamination": 0.95,
        "mean_fragmentation": 0.95,
        "mean_exact_core_families_count_per_genome": 40.0,
        "mean_soft_core_families_count_per_genome": 40.0,
        "mean_persistent_families_count_per_genome": 40.0,
        "mean_shell_families_count_per_genome": 40.0,
        "mean_cloud_families_count_per_genome": 40.0,
    }

    # Return as list of dicts
    return metric1  # [metric1, metric2]


@pytest.fixture
def test_data(
    session: Session,
    pangenome_metric_data: Dict[str, Any],
    release: CollectionRelease,
    tmp_path: Path,
):
    # Create test pangenomes
    taxonomy_source = TaxonomySource(name="NCBI", ranks="Domain")
    taxa = [
        Taxon(name="Bacteria", rank="Domain", depth=0, taxonomy_source=taxonomy_source)
    ]
    pangneom1_file = tmp_path / "PangenomeOne.h5"
    pangneom1_file.write_text("pangenome content")

    pangneom2_file = tmp_path / "PangenomeTwo.h5"
    pangneom2_file.write_text("pangenome content")

    pangenome1 = Pangenome(
        **pangenome_metric_data,
        file_name=pangneom1_file.name,
        annotation_source="PPANGGOLIN",
        collection_release=release,
        taxa=taxa,
        name="Pangenome_test",
        file_md5sum="1a",
    )
    pangenome2 = Pangenome(
        **pangenome_metric_data,
        file_name=pangneom2_file.name,
        annotation_source="PPANGGOLIN",
        collection_release=release,
        name="Pangenome_test2",
        file_md5sum="2a",
        taxa=taxa,
    )

    session.add_all([pangenome1, pangenome2])
    session.commit()


def test_get_pangenomes(client: TestClient, test_data: None):
    response = client.get("/pangenomes/")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["genome_count"] == 50


def test_get_existing_pangenome(client: TestClient, test_data: None):
    # Existing pangenome
    response = client.get("/pangenomes/1")
    assert response.status_code == 200
    data = response.json()
    assert data["genome_count"] == 50


def test_get_non_existing_pangenome(client: TestClient):
    # Non-existent pangenome
    response = client.get("/pangenomes/999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Pangenome not found"


def test_get_pangenome_file_success(
    client: TestClient, session: Session, test_data: None
):
    # Arrange
    pangenome_id = 1  # Assuming this ID exists in test_data

    response = client.get(f"/pangenomes/{pangenome_id}/file")

    # Assert
    assert response.status_code == 200
    assert (
        response.headers["content-disposition"] == 'attachment; filename="pangenome.h5"'
    )


def test_get_pangenome_file_not_found(client: TestClient, session: Session):
    # Arrange
    pangenome_id = 9999  # Assuming this ID does not exist

    # Act
    response = client.get(f"/pangenomes/{pangenome_id}/file")

    # Assert
    assert response.status_code == 404
    assert response.json() == {"detail": "Pangenome not found"}


def test_get_pangenome_file_not_exists(
    client: TestClient, session: Session, test_data: None
):
    # Arrange
    pangenome_id = 1  # Assuming this ID exists in test_data

    # Mock the file's non-existence
    with patch("pathlib.Path.exists", return_value=False):
        # Act
        response = client.get(f"/pangenomes/{pangenome_id}/file")

        # Assert
        assert response.status_code == 404
        assert "does not exists" in response.json()["detail"]
