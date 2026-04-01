from datetime import datetime
from pathlib import Path
import gzip
import pytest
from sqlmodel import Session, select

from pangbank_api.manage_db.genome_status import (
    parse_genome_status_file,
    add_genome_statuses_to_release,
)
from pangbank_api.manage_db.input_models import GenomeStatusInput
from pangbank_api.models import (
    Collection,
    CollectionRelease,
    Genome,
    GenomeSource,
    GenomeStatus,
    TaxonomySource,
)

from ..mock_session import session_fixture  # type: ignore # noqa: F401 # pylint: disable=unused-import


@pytest.fixture
def genome_source(session: Session):
    """Create a genome source in the database."""
    genome_source = GenomeSource(name="RefSeq")
    session.add(genome_source)
    session.commit()
    session.refresh(genome_source)
    return genome_source


@pytest.fixture
def genomes(session: Session, genome_source: GenomeSource) -> dict[str, Genome]:
    """Create test genomes in the database."""
    genome_names = ["GCA_000001.1", "GCA_000002.1", "GCA_000003.1"]
    genomes_list: list[Genome] = []
    
    for name in genome_names:
        genome = Genome(name=name, genome_source_id=genome_source.id)
        session.add(genome)
        genomes_list.append(genome)
    
    session.commit()
    for genome in genomes_list:
        session.refresh(genome)
    
    return {g.name: g for g in genomes_list}


@pytest.fixture
def collection_release(session: Session):
    """Create a collection and release in the database."""
    # Create taxonomy source
    taxonomy_source = TaxonomySource(
        name="GTDB",
        ranks="Domain;Phylum;Class;Order;Family;Genus;Species",
        version="R220",
    )
    session.add(taxonomy_source)
    session.commit()
    session.refresh(taxonomy_source)
    
    # Create collection
    collection = Collection(name="Test Collection")
    session.add(collection)
    session.commit()
    session.refresh(collection)
    
    # Create release
    release = CollectionRelease(
        version="1.0",
        date=datetime(2021, 1, 1),
        ppanggolin_version="3.0.0",
        pangbank_wf_version="1.0.0",
        release_note="Test release",
        mash_sketch="mash_sketch/test.msh",
        mash_version="2.3",
        pangenomes_directory="pangenomes",
        mash_sketch_md5sum="abc123",
        collection_id=collection.id,
        taxonomy_source_id=taxonomy_source.id,
    )
    session.add(release)
    session.commit()
    session.refresh(release)
    
    return release


def test_parse_genome_status_file_plain_text(tmp_path: Path):
    """Test parsing a plain text file with genome names."""
    status_file = tmp_path / "representatives.txt"
    status_file.write_text("GCA_000001.1\nGCA_000002.1\nGCA_000003.1\n")
    
    genome_names = list(parse_genome_status_file(status_file, disable_track=True))
    
    assert len(genome_names) == 3
    assert genome_names[0] == "GCA_000001.1"
    assert genome_names[1] == "GCA_000002.1"
    assert genome_names[2] == "GCA_000003.1"


def test_parse_genome_status_file_gzipped(tmp_path: Path):
    """Test parsing a gzipped file with genome names."""
    status_file = tmp_path / "representatives.txt.gz"
    
    with gzip.open(status_file, "wt") as f:
        f.write("GCA_000001.1\nGCA_000002.1\nGCA_000003.1\n")
    
    genome_names = list(parse_genome_status_file(status_file, disable_track=True))
    
    assert len(genome_names) == 3
    assert genome_names[0] == "GCA_000001.1"
    assert genome_names[1] == "GCA_000002.1"
    assert genome_names[2] == "GCA_000003.1"


def test_parse_genome_status_file_with_empty_lines(tmp_path: Path):
    """Test parsing file with empty lines (should be skipped)."""
    status_file = tmp_path / "representatives.txt"
    status_file.write_text("GCA_000001.1\n\nGCA_000002.1\n\n\nGCA_000003.1\n")
    
    genome_names = list(parse_genome_status_file(status_file, disable_track=True))
    
    assert len(genome_names) == 3


def test_add_genome_statuses_to_release(
    session: Session,
    collection_release: CollectionRelease,
    genomes: dict[str, Genome],
    tmp_path: Path,
):
    """Test adding genome statuses to a release."""
    # Create status file
    status_file = tmp_path / "gtdb_representatives.txt"
    status_file.write_text("GCA_000001.1\nGCA_000002.1\n")
    
    status_input = GenomeStatusInput(
        status_type="representative",
        origin="GTDB RS220",
        file=status_file,
    )
    
    add_genome_statuses_to_release(
        collection_release,
        [status_input],
        genomes,
        session,
    )
    
    # Verify statuses were added
    statuses = session.exec(select(GenomeStatus)).all()
    assert len(statuses) == 2
    
    # Check first status
    assert statuses[0].genome_id == genomes["GCA_000001.1"].id
    assert statuses[0].collection_release_id == collection_release.id
    assert statuses[0].status_type == "representative"
    assert statuses[0].origin == "GTDB RS220"
    
    # Check second status
    assert statuses[1].genome_id == genomes["GCA_000002.1"].id
    assert statuses[1].status_type == "representative"


def test_add_genome_statuses_with_missing_genomes(
    session: Session,
    collection_release: CollectionRelease,
    genomes: dict[str, Genome],
    tmp_path: Path,
):
    """Test that missing genomes are skipped without error."""
    # Create status file with some missing genomes
    status_file = tmp_path / "references.txt"
    status_file.write_text(
        "GCA_000001.1\nGCA_000999.1\nGCA_000002.1\nGCA_000888.1\n"
    )
    
    status_input = GenomeStatusInput(
        status_type="reference",
        origin="NCBI RefSeq",
        file=status_file,
    )
    
    add_genome_statuses_to_release(
        collection_release,
        [status_input],
        genomes,
        session,
    )
    
    # Verify only 2 statuses were added (the ones that exist)
    statuses = session.exec(select(GenomeStatus)).all()
    assert len(statuses) == 2
    assert all(s.status_type == "reference" for s in statuses)
    assert all(s.origin == "NCBI RefSeq" for s in statuses)


def test_add_multiple_genome_statuses(
    session: Session,
    collection_release: CollectionRelease,
    genomes: dict[str, Genome],
    tmp_path: Path,
):
    """Test adding multiple status types to the same genome."""
    # Create files for different status types
    gtdb_file = tmp_path / "gtdb.txt"
    gtdb_file.write_text("GCA_000001.1\n")
    
    refseq_file = tmp_path / "refseq.txt"
    refseq_file.write_text("GCA_000001.1\n")
    
    status_inputs = [
        GenomeStatusInput(
            status_type="representative",
            origin="GTDB RS220",
            file=gtdb_file,
        ),
        GenomeStatusInput(
            status_type="reference",
            origin="NCBI RefSeq",
            file=refseq_file,
        ),
    ]
    
    add_genome_statuses_to_release(
        collection_release,
        status_inputs,
        genomes,
        session,
    )
    
    # Verify both statuses were added
    statuses = session.exec(select(GenomeStatus)).all()
    assert len(statuses) == 2
    
    # Check that the same genome has both statuses
    genome_id = genomes["GCA_000001.1"].id
    assert all(s.genome_id == genome_id for s in statuses)
    
    # Check different status types
    status_types = {s.status_type for s in statuses}
    assert status_types == {"representative", "reference"}


def test_add_genome_statuses_empty_input(
    session: Session,
    collection_release: CollectionRelease,
    genomes: dict[str, Genome],
):
    """Test that empty status inputs list is handled gracefully."""
    add_genome_statuses_to_release(
        collection_release,
        [],
        genomes,
        session,
    )
    
    # Verify no statuses were added
    statuses = session.exec(select(GenomeStatus)).all()
    assert len(statuses) == 0
