from typer.testing import CliRunner

from app.manage_db.genome_metadata import add
from app.models import Genome, GenomeMetadata

import pytest
import tempfile
import json
import random
from pathlib import Path
from unittest.mock import patch
from sqlmodel import Session, SQLModel, create_engine, select
from sqlmodel.pool import StaticPool

runner = CliRunner()


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


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
    header = "Genome\tFeature1\tFeature2\n"
    rows = [
        f"Genome_{i}\t{round(random.uniform(1, 10), 2)}\t{round(random.uniform(10, 20), 2)}\n"
        for i in range(1, 6)
    ]

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".tsv") as tsv_file:
        tsv_file.write(header)
        tsv_file.writelines(rows)
        tsv_file_path = tsv_file.name

    yield tsv_file_path


def test_app(session: Session, metadata_source_file: str, metadata_file: str):

    genomes = [Genome(name=f"Genome_{i}") for i in range(1, 6)]

    session.add_all(genomes)
    session.commit()

    # Mock get_all_genomes_in_pangenome to return the same genomes we inserted
    with patch("app.manage_db.genome_metadata.get_all_genomes_in_pangenome", return_value=genomes):
        with patch("app.manage_db.genome_metadata.Session", return_value=session):
            with patch("app.manage_db.genome_metadata.create_db_and_tables"):
                add(Path(metadata_source_file), Path(metadata_file))

    metadata = session.exec(select(GenomeMetadata)).all()

    assert len(metadata) == 10 # 6 genomes * 2 features
