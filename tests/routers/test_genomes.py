import pytest
from fastapi.testclient import TestClient
from httpx import Response
from sqlmodel import Session


from pangbank_api.models import GenomeSource, Taxon, Genome, TaxonomySource
from ..mock_session import session_fixture, client_fixture  # type: ignore # noqa: F401 # pylint: disable=unused-import


@pytest.fixture
def mock_data(
    session: Session,
):
    # Create mock Pangenome and related models for the tests

    taxonomy_source = TaxonomySource(name="TaxSouce", ranks="Domain;Family;Species")

    taxon_bact = Taxon(
        name="d__Bacteria", rank="Domain", depth=0, taxonomy_source=taxonomy_source
    )
    taxon_actino = Taxon(
        name="p__Actinobacteria",
        rank="Phylum",
        depth=1,
        taxonomy_source=taxonomy_source,
    )
    taxon_archaea = Taxon(
        name="d__Archaea", rank="Domain", depth=0, taxonomy_source=taxonomy_source
    )

    taxonomy_source2 = TaxonomySource(name="TaxSouce", ranks="Domain;Family;Species")

    taxon_bact_2 = Taxon(
        name="Bacteria", rank="Domain", depth=0, taxonomy_source=taxonomy_source2
    )

    genome_source1 = GenomeSource(name="GenomeSource1")
    genome_source2 = GenomeSource(name="GenomeSource2")

    genomeA = Genome(
        name="GenomeArch", taxa=[taxon_archaea], genome_source=genome_source1
    )
    genomeB = Genome(
        name="GenomeB", taxa=[taxon_bact, taxon_bact_2], genome_source=genome_source1
    )
    genomeB2 = Genome(
        name="GenomeB2", taxa=[taxon_bact_2], genome_source=genome_source2
    )

    genomeActino = Genome(
        name="GenomeActino",
        taxa=[taxon_bact, taxon_actino],
        genome_source=genome_source1,
    )

    session.add_all(
        [
            genome_source1,
            genome_source2,
            taxon_bact,
            taxon_actino,
            taxon_archaea,
            taxon_bact_2,
            genomeA,
            genomeB,
            genomeB2,
            genomeActino,
        ]
    )
    session.commit()


def test_read_genomes_success(client: TestClient, session: Session, mock_data: None):
    """
    Test to ensure that the /genomes/ endpoint returns a list of genomes.
    """
    # Act
    response = client.get("/genomes/")

    # Assert
    assert response.status_code == 200
    data: Response = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert "name" in data[0]
    assert "taxonomies" in data[0]


def test_read_genomes_filter_by_name(
    client: TestClient, session: Session, mock_data: None
):
    """
    Test the /genomes/ endpoint with a filter on genome name.
    """
    # Arrange
    genome_name = "GenomeArch"

    # Act
    response = client.get(f"/genomes/?genome_name={genome_name}")

    # Assert
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["name"] == genome_name


def test_read_genomes_filter_by_taxon(
    client: TestClient, session: Session, mock_data: None
):
    """
    Test the /genomes/ endpoint with a filter on genome name.
    """
    # Arrange
    taxon_name = "Bacteria"

    # Act
    response = client.get(f"/genomes/?taxon_name={taxon_name}")

    # Assert
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2  # Two genomes have the taxon "Bacteria"


def test_read_genomes_filter_by_substring_match_taxon(
    client: TestClient, session: Session, mock_data: None
):
    """
    Test the /genomes/ endpoint with a filter on genome name.
    """
    # Arrange
    taxon_name = "Bacteria"

    # Act
    response = client.get(
        f"/genomes/?taxon_name={taxon_name}&substring_taxon_match=true"
    )

    # Assert
    assert response.status_code == 200
    data = response.json()
    # Two genomes have the taxon "Bacteria" and one has "d__Bacteria"
    assert len(data) == 3


def test_read_genomes_pagination(client: TestClient, session: Session, mock_data: None):
    """
    Test pagination by limiting the number of results.
    """
    # Act
    response = client.get("/genomes/?limit=1&offset=0")

    # Assert
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1


def test_get_genome_by_id_success(
    client: TestClient, session: Session, mock_data: None
):
    """
    Test fetching a genome by its ID.
    """
    # Arrange
    genome_id = 1  # Assuming this ID exists

    # Act
    response = client.get(f"/genomes/{genome_id}")

    # Assert
    assert response.status_code == 200
    data = response.json()
    assert "name" in data
    assert "taxonomies" in data


def test_get_genome_by_id_not_found(client: TestClient, session: Session):
    """
    Test fetching a genome with a non-existent ID.
    """
    # Arrange
    genome_id = 9999  # Assuming this ID does not exist

    # Act
    response = client.get(f"/genomes/{genome_id}")

    # Assert
    assert response.status_code == 404
    assert response.json() == {"detail": "Genome not found"}
