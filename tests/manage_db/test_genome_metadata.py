from pangbank_api.manage_db.genome_metadata import add, delete, list
from pangbank_api.models import Genome, GenomeMetadata, GenomeMetadataSource

import pytest
import tempfile
import json
import random
from pathlib import Path
from unittest.mock import patch
from sqlmodel import Session, select

from tests.mock_session import session_fixture  # type: ignore # noqa: F401 # pylint: disable=unused-import


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


def test_add_genome_metadata(
    session: Session, metadata_source_file: str, metadata_file: str
):

    genomes = [Genome(name=f"Genome_{i}") for i in range(1, 6)]

    session.add_all(genomes)
    session.commit()

    # Mock get_all_genomes_in_pangenome to return the same genomes we inserted
    with patch(
        "app.manage_db.genome_metadata.get_all_genomes_in_pangenome",
        return_value=genomes,
    ):
        with patch("app.manage_db.genome_metadata.Session", return_value=session):
            with patch("app.manage_db.genome_metadata.create_db_and_tables"):
                add(Path(metadata_source_file), Path(metadata_file))

    # Try adding the same metadata source again
    with patch(
        "app.manage_db.genome_metadata.get_all_genomes_in_pangenome",
        return_value=genomes,
    ):
        with patch("app.manage_db.genome_metadata.Session", return_value=session):
            with patch("app.manage_db.genome_metadata.create_db_and_tables"):
                with pytest.raises(ValueError):
                    add(Path(metadata_source_file), Path(metadata_file))

    metadata = session.exec(select(GenomeMetadata)).all()

    assert len(metadata) == 10

    metadata = session.exec(select(GenomeMetadataSource)).all()
    assert len(metadata) == 1

    with patch("app.manage_db.genome_metadata.Session", return_value=session):
        with patch("app.manage_db.genome_metadata.create_db_and_tables"):
            delete("DB_A", "2.6.0")

    metadata = session.exec(select(GenomeMetadataSource)).all()
    assert len(metadata) == 0


def test_delete_unexisiting_genome_metadata(session: Session):
    with patch("app.manage_db.genome_metadata.Session", return_value=session):
        with patch("app.manage_db.genome_metadata.create_db_and_tables"):
            with pytest.raises(ValueError):
                delete("UNEXISTING_SOURCE")


def test_list_metadata_source(
    session: Session, capsys: pytest.CaptureFixture  # type: ignore
):

    source = GenomeMetadataSource(name="DB_A", version="2.6.0")
    session.add(source)
    session.commit()

    with patch("app.manage_db.genome_metadata.Session", return_value=session):
        list()

    captured = capsys.readouterr()  # type: ignore
    assert "DB_A" in captured.out  # type: ignore
    assert "2.6.0" in captured.out  # type: ignore


def test_list_metadata_source_empty_db(
    session: Session, capsys: pytest.CaptureFixture  # type: ignore
):

    with patch("app.manage_db.genome_metadata.Session", return_value=session):
        list()

    captured = capsys.readouterr()  # type: ignore
    assert "No genome metadata sources found in the database." in captured.out  # type: ignore
