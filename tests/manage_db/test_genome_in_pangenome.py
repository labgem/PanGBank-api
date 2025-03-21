from typing import Any, Dict
from pangbank_api.models import (
    Genome,
    GenomeInPangenomeMetadata,
    GenomeInPangenomeMetadataSource,
    GenomePangenomeLink,
    Pangenome,
)

import pytest
from sqlmodel import Session, select
from ..mock_session import session_fixture  # type: ignore # noqa: F401 # pylint: disable=unused-import
from ..mock_data import (
    mock_data,  # type: ignore # noqa: F401 # pylint: disable=unused-import
    pangenome_metric_data,  # type: ignore # noqa: F401 # pylint: disable=unused-import
    genome_in_pangenome_metric_data,  # type: ignore # noqa: F401 # pylint: disable=unused-import
)


def test_add_genome_in_pangenome_metadata(session: Session, mock_data: None):
    """Test with no filters applied, should return all pangenomes."""

    genome_pangenome_link = session.exec(select(GenomePangenomeLink)).first()

    if genome_pangenome_link is None:
        pytest.fail("No GenomePangenomeLink found in the database")

    metadata_source = GenomeInPangenomeMetadataSource(name="DB_A")

    metadata = GenomeInPangenomeMetadata(
        key="Feature1",
        value="1",
        source=metadata_source,
        genome_in_pangenome=genome_pangenome_link,
    )

    session.add(metadata_source)
    session.add(metadata)
    session.commit()

    metadata = session.exec(select(GenomeInPangenomeMetadata)).first()
    assert metadata is not None
    assert metadata.key == "Feature1"
    assert metadata.value == "1"
    assert metadata.source.name == "DB_A"
    assert metadata.genome_in_pangenome.genome_id is not None
    assert metadata.genome_in_pangenome.genome_id == genome_pangenome_link.genome_id
    assert metadata.genome_pangenome_link_id == genome_pangenome_link.id

    session.delete(genome_pangenome_link)
    session.commit()

    metadata = session.exec(select(GenomeInPangenomeMetadata)).first()
    assert metadata is None


def test_genome_in_pangenome_uniqness(
    session: Session, mock_data: None, genome_in_pangenome_metric_data: Dict[str, Any]
):
    """Test with no filters applied, should return all pangenomes."""

    pangenome = session.exec(select(Pangenome)).first()

    if pangenome is None:
        pytest.fail("No Pangenome found in the database")

    genome = Genome(name="GenomeX")

    genome_pangenome_link = GenomePangenomeLink(
        **genome_in_pangenome_metric_data,
        genome=genome,
        pangenome=pangenome,
        genome_file_md5sum="a6c41b3f5b5faff3cd98d1566a79cdb2",
        genome_file_name="genomeA.fasta",
    )

    session.add(genome)
    session.add(genome_pangenome_link)
    session.commit()

    session.refresh(genome)

    genome_pangenome_link = session.exec(
        select(GenomePangenomeLink).join(Genome).where(Genome.name == "GenomeX")
    ).one()

    assert genome_pangenome_link is not None

    genome_pangenome_link_duplicate = GenomePangenomeLink(
        **genome_in_pangenome_metric_data,
        genome=genome,
        pangenome=pangenome,
        genome_file_md5sum="XXXX",
        genome_file_name="genomeA.fasta",
    )

    session.add(genome_pangenome_link_duplicate)
    with pytest.raises(Exception):
        session.commit()
