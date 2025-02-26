import pytest
from sqlmodel import Session, select

from pathlib import Path

from app.manage_db.genomes import add_genomes_to_db
from app.models import Genome, GenomeSourceInput

from tests.mock_session import session_fixture  # type: ignore # noqa: F401 # pylint: disable=unused-import


@pytest.fixture
def genome_source_info(tmp_path: Path):

    genome_source_file = tmp_path / "RefSeq.list"
    genome_source_file.write_text("Genome1\nGenome2\nGenome3\n")

    genome_source_info = GenomeSourceInput(name="RefSeq", file=genome_source_file)

    return genome_source_info


def test_add_genomes_to_db(session: Session, genome_source_info: GenomeSourceInput):

    add_genomes_to_db([genome_source_info], session)

    genomes = session.exec(select(Genome)).all()
    assert len(genomes) == 3
    assert genomes[0].name == "Genome1"
    assert genomes[1].name == "Genome2"
    assert genomes[2].name == "Genome3"
