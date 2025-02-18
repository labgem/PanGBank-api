from typing import Any, Dict

import pytest
from sqlmodel import Session, SQLModel, create_engine, select
from sqlmodel.pool import StaticPool

from app.crud.common import FilterPangenome, PaginationParams
from app.crud.pangenomes import get_pangenomes
from app.models import (
    Genome,
    GenomePangenomeLink,
    Pangenome,
    Taxon,
    TaxonomySource,
)


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
    }

    # Return as list of dicts
    return metric1  # [metric1, metric2]


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


@pytest.fixture(name="genome_in_pangenome_metric_data")
def generate_genome_in_pangenome_metric_data() -> Dict[str, Any]:
    return {
        "Genome_name": "GenomeA",
        "Contigs": 50,
        "Genes": 1000,
        "Fragmented_genes": 50,
        "Families": 150,
        "Families_with_fragments": 10,
        "Families_in_multicopy": 5,
        "Soft_core_families": 80,
        "Soft_core_genes": 800,
        "Exact_core_families": 70,
        "Exact_core_genes": 700,
        "Persistent_genes": 900,
        "Persistent_fragmented_genes": 45,
        "Persistent_families": 140,
        "Persistent_families_with_fragments": 8,
        "Persistent_families_in_multicopy": 4,
        "Shell_genes": 60,
        "Shell_fragmented_genes": 30,
        "Shell_families": 90,
        "Shell_families_with_fragments": 5,
        "Shell_families_in_multicopy": 3,
        "Cloud_genes": 40,
        "Cloud_fragmented_genes": 20,
        "Cloud_families": 70,
        "Cloud_families_with_fragments": 3,
        "Cloud_families_in_multicopy": 2,
        "Completeness": 0.95,
        "Contamination": 0.02,
        "Fragmentation": 0.05,
        "RGPs": 10,
        "Spots": 5,
        "Modules": 8,
    }


@pytest.fixture
def mock_data(
    session: Session,
    pangenome_metric_data: Dict[str, Any],
    genome_in_pangenome_metric_data: Dict[str, Any],
):
    # Create mock Pangenome and related models for the test

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

    pangenome1 = Pangenome(
        **pangenome_metric_data,
        collection_release_id=1,
        file_name="Pangenome One",
        taxa=[taxon_bact, taxon_actino],
    )
    pangenome2 = Pangenome(
        **pangenome_metric_data,
        collection_release_id=1,
        file_name="Pangenome Two",
        taxa=[taxon_archaea],
    )
    pangenome3 = Pangenome(
        **pangenome_metric_data,
        collection_release_id=2,
        file_name="Pangenome Three",
        taxa=[taxon_bact_2],
    )

    genomeA = Genome(name="GenomeA")

    genome_pangenome_link = GenomePangenomeLink(
        **genome_in_pangenome_metric_data,
        genome=genomeA,
        pangenome=pangenome1,
        genome_file_md5sum="a6c41b3f5b5faff3cd98d1566a79cdb2",
        genome_file_name="genomeA.fasta",
    )

    genome_pangenome_link2 = GenomePangenomeLink(
        **genome_in_pangenome_metric_data,
        genome=genomeA,
        pangenome=pangenome3,
        genome_file_md5sum="a6c41b3f5b5faff3cd98d1566a79cdb2",
        genome_file_name="genomeA.fasta",
    )

    session.add_all(
        [
            pangenome1,
            pangenome2,
            pangenome3,
            genomeA,
            genome_pangenome_link,
            genome_pangenome_link2,
        ]
    )
    session.commit()


def test_get_pangenomes_no_filters(session: Session, mock_data: None):
    """Test with no filters applied, should return all pangenomes."""
    empty_filter_params = FilterPangenome()

    result = get_pangenomes(
        session=session, filter_params=empty_filter_params, pagination_params=None
    )
    assert len(result) == 3  # Expecting all 3 pangenomes
    assert all(isinstance(p, Pangenome) for p in result)


def test_get_pangenomes_with_collection_release_filter(
    session: Session, mock_data: None
):
    """Test with collection_release_id filter."""
    filter_params = FilterPangenome(collection_release_id=1)
    result = get_pangenomes(
        session=session, filter_params=filter_params, pagination_params=None
    )
    assert len(result) == 2  # Only 2 pangenomes with collection_release_id=1
    assert all(p.collection_release_id == 1 for p in result)


def test_get_pangenomes_with_genome_name_filter(session: Session, mock_data: None):
    """Test with genome_name filter."""
    filter_params = FilterPangenome(genome_name="GenomeA")

    result = get_pangenomes(session=session, filter_params=filter_params)

    assert len(result) == 2


def test_get_pangenomes_with_taxon_name_filter(session: Session, mock_data: None):
    """Test with exact taxon_name filter."""

    filter_params = FilterPangenome(taxon_name="d__Bacteria")
    result = get_pangenomes(
        session=session, filter_params=filter_params, pagination_params=None
    )
    assert len(result) == 1


def test_get_pangenomes_with_taxon_name_substring_filter(
    session: Session, mock_data: None
):
    """Test with taxon_name substring filter."""

    filter_params = FilterPangenome(taxon_name="Bacteria", substring_match=True)
    result = get_pangenomes(
        session=session, filter_params=filter_params, pagination_params=None
    )

    assert len(result) == 2  # Should match only the taxon that contains "Bact"


def test_get_pangenomes_with_pagination(session: Session, mock_data: None):
    """Test with pagination (offset and limit)."""
    pagination_params = PaginationParams(offset=0, limit=1)
    empty_filter_params = FilterPangenome()

    result = get_pangenomes(
        session=session,
        filter_params=empty_filter_params,
        pagination_params=pagination_params,
    )
    assert len(result) == 1  # With pagination, only one result should be returned


def test_get_pangenomes_with_combined_filters(session: Session, mock_data: None):
    """Test with multiple filters applied."""

    filter_params = FilterPangenome(collection_release_id=1, genome_name="GenomeA")

    result = get_pangenomes(session=session, filter_params=filter_params)
    assert len(result) == 1  # Only one result should match the combined filters
    assert result[0].collection_release_id == 1


def test_get_pangenomes_no_results(session: Session):
    """Test when no results match the filters."""

    filter_params = FilterPangenome(collection_release_id=999)

    result = get_pangenomes(
        session=session, filter_params=filter_params, pagination_params=None
    )
    assert len(result) == 0  # Should return no
