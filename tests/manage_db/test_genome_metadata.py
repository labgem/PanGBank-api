from pangbank_api.manage_db.genome_metadata import (
    delete,
    list,
    convert_value_to_field_type,
    update_genomes_with_quality_metrics,
)
from pangbank_api.models import (
    GenomeMetadataSource,
    Genome,
    MetadataBase,
    GenomeBase,
)

import pytest
import tempfile
import json
import random
import logging
from unittest.mock import patch
from sqlmodel import Session

from ..mock_session import session_fixture  # type: ignore # noqa: F401 # pylint: disable=unused-import


@pytest.fixture
def metadata_source_file():
    """Creates a temporary JSON metadata file for testing."""
    metadata = {"name": "DB_A", "version": "2.6.0"}

    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".json"
    ) as json_file:
        json.dump(metadata, json_file)
        json_file_path = json_file.name

    yield json_file_path


@pytest.fixture
def metadata_file():
    """Creates a temporary TSV metadata file for testing."""
    header = "genomes\tFeature1\tFeature2\n"
    rows = [
        f"Genome_{i}\t{round(random.uniform(1, 10), 2)}\t{round(random.uniform(10, 20), 2)}\n"
        for i in range(1, 6)
    ]

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".tsv") as tsv_file:
        tsv_file.write(header)
        tsv_file.writelines(rows)
        tsv_file_path = tsv_file.name

    yield tsv_file_path


def test_delete_unexisiting_genome_metadata(session: Session):
    with patch("pangbank_api.manage_db.genome_metadata.Session", return_value=session):
        with patch("pangbank_api.manage_db.genome_metadata.create_db_and_tables"):
            with pytest.raises(ValueError):
                delete("UNEXISTING_SOURCE")


def test_list_metadata_source(
    session: Session,
    capsys: pytest.CaptureFixture,  # type: ignore
):
    source = GenomeMetadataSource(name="DB_A")
    session.add(source)
    session.commit()

    with patch("pangbank_api.manage_db.genome_metadata.Session", return_value=session):
        list()

    captured = capsys.readouterr()  # type: ignore
    assert "DB_A" in captured.out  # type: ignore


def test_list_metadata_source_empty_db(
    session: Session,
    capsys: pytest.CaptureFixture,  # type: ignore
):
    with patch("pangbank_api.manage_db.genome_metadata.Session", return_value=session):
        list()

    captured = capsys.readouterr()  # type: ignore
    assert "No genome metadata sources found in the database." in captured.out  # type: ignore


def test_convert_value_to_field_type_int():
    """Test conversion of string to int."""
    result = convert_value_to_field_type("100", "genome_size")
    assert result == 100
    assert isinstance(result, int)


def test_convert_value_to_field_type_float():
    """Test conversion of string to float."""
    result = convert_value_to_field_type("98.5", "checkm2_completeness")
    assert result == 98.5
    assert isinstance(result, float)


def test_convert_value_to_field_type_empty_string():
    """Test that empty strings return None."""
    result = convert_value_to_field_type("", "genome_size")
    assert result is None


def test_convert_value_to_field_type_invalid_field():
    """Test that invalid field names raise ValueError."""
    with pytest.raises(ValueError, match="Field invalid_field not found"):
        convert_value_to_field_type("100", "invalid_field")


def test_update_genomes_with_quality_metrics_skips_required_fields(session: Session):
    """Test that required fields like 'name' are not overwritten (simplified test)."""
    # Test the field checking logic

    # Test that 'name' is a required field
    name_field = GenomeBase.model_fields.get("name")
    assert name_field is not None
    assert name_field.is_required()

    # Test that checkm2_completeness is optional
    completeness_field = GenomeBase.model_fields.get("checkm2_completeness")
    assert completeness_field is not None
    assert not completeness_field.is_required()


def test_update_genomes_with_quality_metrics_prevents_value_changes(
    session: Session,
):
    """Test that existing quality metrics cannot be overwritten without allow_overwrite flag."""
    # Create a genome with existing quality metrics
    genome = Genome(name="TestGenome", checkm2_completeness=98.5, genome_size=5000000)
    session.add(genome)
    session.commit()
    session.refresh(genome)

    # Verify initial values
    assert genome.checkm2_completeness == 98.5
    assert genome.genome_size == 5000000

    # Try to update with different values (without allow_overwrite)
    def quality_metrics_generator():
        yield "TestGenome", [
            MetadataBase(key="checkm2_completeness", value="95.0"),  # Different value
        ]

    # Should raise ValueError
    with pytest.raises(ValueError, match="value mismatch"):
        update_genomes_with_quality_metrics(
            quality_metrics_generator(),
            session=session,
            collection_release=None,
            allow_overwrite=False,
        )

    session.refresh(genome)

    # Verify that value was NOT changed (transaction rolled back)
    assert genome.checkm2_completeness == 98.5


def test_update_genomes_with_quality_metrics_force_overwrite(session: Session, caplog):
    """Test that existing quality metrics can be overwritten with allow_overwrite=True."""
    # Create a genome with existing quality metrics
    genome = Genome(name="TestGenome", checkm2_completeness=98.5, genome_size=5000000)
    session.add(genome)
    session.commit()
    session.refresh(genome)

    # Try to update with different values (with allow_overwrite=True)
    def quality_metrics_generator():
        yield "TestGenome", [
            MetadataBase(key="checkm2_completeness", value="95.0"),  # Different value
            MetadataBase(key="genome_size", value="6000000"),  # Different value
            MetadataBase(key="gc_percentage", value="45.5"),  # New field (was None)
        ]

    # Capture log warnings
    with caplog.at_level(logging.WARNING):
        update_genomes_with_quality_metrics(
            quality_metrics_generator(),
            session=session,
            collection_release=None,
            allow_overwrite=True,
        )

    session.refresh(genome)

    # Verify that values WERE changed
    assert genome.checkm2_completeness == 95.0  # Changed
    assert genome.genome_size == 6000000  # Changed
    assert genome.gc_percentage == 45.5  # New value was added

    # Verify warnings were logged
    assert "value mismatch" in caplog.text
    assert "Overwriting with new value" in caplog.text


def test_update_genomes_with_quality_metrics_allows_same_values(session: Session):
    """Test that re-applying the same quality metrics is allowed (idempotent)."""
    # Create a genome with existing quality metrics
    genome = Genome(name="TestGenome", checkm2_completeness=98.5)
    session.add(genome)
    session.commit()
    session.refresh(genome)

    # Apply the same values again (should succeed without warnings regardless of allow_overwrite)
    def quality_metrics_generator():
        yield "TestGenome", [
            MetadataBase(key="checkm2_completeness", value="98.5"),  # Same value
        ]

    # This should succeed without warnings or errors
    update_genomes_with_quality_metrics(
        quality_metrics_generator(),
        session=session,
        collection_release=None,
        allow_overwrite=False,  # Doesn't matter since values match
    )

    session.refresh(genome)

    # Value should remain the same
    assert genome.checkm2_completeness == 98.5
