from sqlmodel import Session

from pangbank_api.crud.common import FilterCollectionTaxonGenome, PaginationParams
from pangbank_api.crud.pangenomes import get_pangenomes
from pangbank_api.models import Pangenome
from ..mock_session import session_fixture  # type: ignore # noqa: F401 # pylint: disable=unused-import
from ..mock_data import (
    mock_data,  # type: ignore # noqa: F401 # pylint: disable=unused-import
    pangenome_metric_data,  # type: ignore # noqa: F401 # pylint: disable=unused-import
    genome_in_pangenome_metric_data,  # type: ignore # noqa: F401 # pylint: disable=unused-import
)


def test_get_pangenomes_no_filters(session: Session, mock_data: None):
    """Test with no filters applied, should return all pangenomes."""
    empty_filter_params = FilterCollectionTaxonGenome()

    result = get_pangenomes(
        session=session, filter_params=empty_filter_params, pagination_params=None
    )
    assert len(result) == 3  # Expecting all 3 pangenomes
    assert all(isinstance(p, Pangenome) for p in result)


def test_get_pangenomes_with_genome_name_filter(session: Session, mock_data: None):
    """Test with genome_name filter."""
    filter_params = FilterCollectionTaxonGenome(genome_name="GenomeA")

    result = get_pangenomes(session=session, filter_params=filter_params)

    assert len(result) == 2


def test_get_pangenomes_with_taxon_name_filter(session: Session, mock_data: None):
    """Test with exact taxon_name filter."""

    filter_params = FilterCollectionTaxonGenome(taxon_name="d__Bacteria")
    result = get_pangenomes(
        session=session, filter_params=filter_params, pagination_params=None
    )
    assert len(result) == 1


def test_get_pangenomes_with_taxon_name_substring_filter(
    session: Session, mock_data: None
):
    """Test with taxon_name substring filter."""

    filter_params = FilterCollectionTaxonGenome(
        taxon_name="Bacteria", substring_match=True
    )
    result = get_pangenomes(
        session=session, filter_params=filter_params, pagination_params=None
    )

    assert len(result) == 2  # Should match only the taxon that contains "Bact"


def test_get_pangenomes_with_pagination(session: Session, mock_data: None):
    """Test with pagination (offset and limit)."""
    pagination_params = PaginationParams(offset=0, limit=1)
    empty_filter_params = FilterCollectionTaxonGenome()

    result = get_pangenomes(
        session=session,
        filter_params=empty_filter_params,
        pagination_params=pagination_params,
    )
    assert len(result) == 1  # With pagination, only one result should be returned


def test_get_pangenomes_with_combined_filters(session: Session, mock_data: None):
    """Test with multiple filters applied."""

    filter_params = FilterCollectionTaxonGenome(
        only_latest_release=True, genome_name="GenomeA"
    )

    result = get_pangenomes(session=session, filter_params=filter_params)
    assert len(result) == 1  # Only one result should match the combined filters
    assert result[0].collection_release.latest is True


def test_get_pangenomes_no_results(session: Session):
    """Test when no results match the filters."""

    filter_params = FilterCollectionTaxonGenome(genome_name="NoValidGenomeName")

    result = get_pangenomes(
        session=session, filter_params=filter_params, pagination_params=None
    )
    assert len(result) == 0  # Should return no
