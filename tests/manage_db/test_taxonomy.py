import pytest
import gzip
from pathlib import Path
from app.manage_db.taxonomy import (
    parse_taxonomy_file,
    add_taxon_to_db,
    link_genomes_and_taxa,
    get_common_taxa,
)

from app.models import TaxonomySource, Taxon, Genome
from sqlmodel import Session, select


from tests.mock_session import session_fixture  # type: ignore # noqa: F401 # pylint: disable=unused-import


@pytest.fixture
def taxonomy_tsv(tmp_path: Path):
    """Creates a valid uncompressed taxonomy file."""
    file = tmp_path / "taxonomy.tsv"
    file.write_text(
        "genome1\tDomain;Phylum;Class;Order;Family;Genus;Species\n"
        "genome2\tDomain;Phylum;Class;Order;Family;Genus;Species\n"
    )
    return file


@pytest.fixture
def taxonomy_tsv_gz(tmp_path: Path):
    """Creates a valid gzipped taxonomy file."""
    file = tmp_path / "taxonomy.tsv.gz"
    with gzip.open(file, "wt") as f:
        f.write(
            "genome1\tDomain;Phylum;Class;Order;Family;Genus;Species\n"
            "genome2\tDomain;Phylum;Class;Order;Family;Genus;Species\n"
        )
    return file


@pytest.fixture
def malformed_taxonomy_file(tmp_path: Path):
    """Creates a malformed taxonomy file (missing tab separator)."""
    file = tmp_path / "malformed_taxonomy.tsv"
    file.write_text("genome1 Domain;Phylum;Class;Order;Family;Genus;Species\n")
    return file


def test_parse_taxonomy_tsv(taxonomy_tsv: Path):
    """Test parsing a valid uncompressed taxonomy file."""
    expected_output = {
        "genome1": ("Domain", "Phylum", "Class", "Order", "Family", "Genus", "Species"),
        "genome2": ("Domain", "Phylum", "Class", "Order", "Family", "Genus", "Species"),
    }
    assert parse_taxonomy_file(taxonomy_tsv) == expected_output


def test_parse_taxonomy_tsv_gz(taxonomy_tsv_gz: Path):
    """Test parsing a valid gzipped taxonomy file."""
    expected_output = {
        "genome1": ("Domain", "Phylum", "Class", "Order", "Family", "Genus", "Species"),
        "genome2": ("Domain", "Phylum", "Class", "Order", "Family", "Genus", "Species"),
    }
    assert parse_taxonomy_file(taxonomy_tsv_gz) == expected_output


def test_parse_taxonomy_malformed(malformed_taxonomy_file: Path):
    """Test that a malformed file (missing tab) raises ValueError."""
    with pytest.raises(ValueError):
        parse_taxonomy_file(malformed_taxonomy_file)


def test_add_taxon_to_db(session: Session):

    taxonomy_source = TaxonomySource(
        name="GTDB",
        version="24.1",
        ranks="Domain; Phylum; Species",
    )

    session.add(taxonomy_source)
    session.commit()
    lineages = {
        ("D1", "P1", "S1a"),
        ("D1", "P1", "S1b"),
    }
    name_to_taxon_by_depth = add_taxon_to_db(taxonomy_source, lineages, session)

    assert len(name_to_taxon_by_depth) == 3
    assert len(name_to_taxon_by_depth[0]) == 1
    assert len(name_to_taxon_by_depth[1]) == 1
    assert len(name_to_taxon_by_depth[2]) == 2
    assert name_to_taxon_by_depth[0]["D1"].name == "D1"
    assert name_to_taxon_by_depth[1]["P1"].name == "P1"
    assert name_to_taxon_by_depth[2]["S1a"].name == "S1a"
    assert name_to_taxon_by_depth[2]["S1b"].name == "S1b"

    # Check that the taxa are in the database
    taxa = session.exec(select(Taxon)).all()
    assert len(taxa) == 4
    taxon = taxa[0]
    assert taxon.name == "D1"
    assert taxon.rank == "Domain"
    assert taxon.depth == 0

    assert taxon.taxonomy_source.name == "GTDB"
    assert taxon.taxonomy_source.version == "24.1"
    assert taxon.taxonomy_source.ranks == "Domain; Phylum; Species"


def test_link_genomes_and_taxa(session: Session):

    genome_a = Genome(name="GenomeA")
    genome_b = Genome(name="GenomeB")

    genome_name_to_genome = {
        "GenomeA": genome_a,
        "GenomeB": genome_b,
    }
    genome_name_to_lineage = {
        "GenomeA": ("D1", "P1", "S1a"),
        "GenomeB": ("D1", "P1", "S1b"),
    }
    name_to_taxon_by_depth = [
        {"D1": Taxon(name="D1", rank="Domain", depth=0)},
        {"P1": Taxon(name="P1", rank="Phylum", depth=1)},
        {
            "S1a": Taxon(name="S1a", rank="Species", depth=2),
            "S1b": Taxon(name="S1b", rank="Species", depth=2),
        },
    ]
    taxa = [taxon for depth in name_to_taxon_by_depth for taxon in depth.values()]
    session.add_all(taxa)
    session.add_all([genome_a, genome_b])

    session.commit()

    session.refresh(genome_a)
    session.refresh(genome_b)
    for taxon in taxa:
        session.refresh(taxon)

    link_genomes_and_taxa(
        genome_name_to_genome=genome_name_to_genome,
        genome_name_to_lineage=genome_name_to_lineage,
        name_to_taxon_by_depth=name_to_taxon_by_depth,
        session=session,
    )
    pass


@pytest.fixture
def sample_taxa() -> list[Taxon]:
    """Provides sample Taxon instances for testing."""
    taxa = [
        Taxon(name="Bacteria", rank="Domain", depth=0),
        Taxon(name="Proteobacteria", rank="Phylum", depth=1),
        Taxon(name="Gammaproteobacteria", rank="Class", depth=2),
    ]

    return taxa


@pytest.fixture
def other_taxa():
    """Provides another set of sample Taxon instances."""
    return [
        Taxon(name="Bacteria", rank="Domain", depth=0),
        Taxon(name="Firmicutes", rank="Phylum", depth=1),
        Taxon(name="Bacilli", rank="Class", depth=2),
    ]


def test_get_common_taxa(sample_taxa: list[Taxon], other_taxa: list[Taxon]):
    """Tests if common taxa are correctly identified."""
    common = get_common_taxa(sample_taxa, other_taxa)

    assert len(common) == 1  # Only "Bacteria" is common
    assert common[0].name == "Bacteria"
    assert common[0].rank == "Domain"


def test_get_common_taxa_no_common():
    """Tests when there are no common taxa."""
    taxa_A = [Taxon(name="Actinobacteria", rank="Phylum", depth=1)]
    taxa_B = [Taxon(name="Firmicutes", rank="Phylum", depth=1)]

    common = get_common_taxa(taxa_A, taxa_B)
    assert common == []


def test_get_common_taxa_identical_lists(sample_taxa: list[Taxon]):
    """Tests when both lists are identical."""
    common = get_common_taxa(sample_taxa, sample_taxa)
    assert len(common) == len(sample_taxa)
    assert common == sample_taxa


def test_get_common_taxa_empty():
    """Tests when one or both lists are empty."""
    assert get_common_taxa([], []) == []
    assert get_common_taxa([], [Taxon(name="Bacteria", rank="Domain", depth=0)]) == []
    assert get_common_taxa([Taxon(name="Bacteria", rank="Domain", depth=0)], []) == []
