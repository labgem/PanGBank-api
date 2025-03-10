from datetime import datetime
from pathlib import Path
import pytest
from pangbank_api.manage_db.collections import (
    create_collection_release,
    add_pangenomes_to_db,
    delete_full_collection,
    delete_collection_release,
    print_collections,
)

from pangbank_api.models import (
    CollectionRelease,
    Collection,
    Genome,
    Pangenome,
    TaxonomySource,
)
from sqlmodel import Session, select
from unittest.mock import patch

import gzip
from tests.mock_session import session_fixture  # type: ignore # noqa: F401 # pylint: disable=unused-import


@pytest.fixture()
def taxonomy_source():
    taxonomy_source = TaxonomySource(name="TaxSouce", ranks="Domain;Family;Species")
    return taxonomy_source


@pytest.fixture()
def collection_release():
    collection_release = CollectionRelease(
        version="1.0",
        date=datetime(2021, 1, 1),
        ppanggolin_version="3.0.0",
        pangbank_wf_version="1.0.0",
        release_note="This is the first release of the collection",
        mash_sketch="mash_sketch/families_persistent_all.msh",
        mash_version="2.3",
        pangenomes_directory="pangenomes",
    )

    return collection_release


@pytest.fixture()
def collection_release2():
    collection_release2 = CollectionRelease(
        version="2.0",
        date=datetime(2021, 1, 1),
        ppanggolin_version="3.0.0",
        pangbank_wf_version="1.0.0",
        release_note="This is the first release of the collection",
        mash_sketch="mash_sketch/families_persistent_all.msh",
        mash_version="2.3",
        pangenomes_directory="pangenomes",
    )
    return collection_release2


@pytest.fixture()
def collection():

    collection = Collection(
        name="collection_A",
    )
    return collection


@pytest.fixture()
def pangenome_dir(tmp_path: Path):
    """Creates a mock pangenome directory with necessary files."""

    # Create main pangenome directory
    main_pangenome_dir = tmp_path / "pangenomes"
    main_pangenome_dir.mkdir(parents=True, exist_ok=True)

    # Create species-specific directory
    pangenome_dir = main_pangenome_dir / "speciesA"
    pangenome_dir.mkdir(parents=True, exist_ok=True)

    # Create and write to `info.yaml`
    info_yaml_content = """\
Content:
    Genes: 12166
    Genomes: 15
    Families: 857
    Edges: 915
    Persistent:
        Family_count: 784
        min_genomes_frequency: 0.73
        max_genomes_frequency: 1.0
        sd_genomes_frequency: 0.02
        mean_genomes_frequency: 1.0
    Shell:
        Family_count: 27
        min_genomes_frequency: 0.6
        max_genomes_frequency: 0.87
        sd_genomes_frequency: 0.06
        mean_genomes_frequency: 0.81
    Cloud:
        Family_count: 46
        min_genomes_frequency: 0.07
        max_genomes_frequency: 0.33
        sd_genomes_frequency: 0.07
        mean_genomes_frequency: 0.12
    Number_of_partitions: 3
    RGP: 2
    Spots: 1
    Modules:
        Number_of_modules: 1
        Families_in_Modules: 5
        Partition_composition:
            Persistent: 0.0
            Shell: 0.0
            Cloud: 100.0
"""
    info_yaml = pangenome_dir / "info.yaml"
    info_yaml.write_text(info_yaml_content)

    # Create `pangenome.h5` as an empty file
    pangenome_h5 = pangenome_dir / "pangenome.h5"
    pangenome_h5.touch()

    # Create and write to `genomes_md5sum.tsv`
    genomes_md5sum_content = """\
name\tfile_name\tmd5_sum
GenomeA\tgenomic.gbff.gz\t3e8884a3ecbd47ab0573ba68257dcdb0
GenomeB\tgenomic.gbff.gz\td0365cda22933ded5864dddfd2aed96f
"""
    genomes_md5sum = pangenome_dir / "genomes_md5sum.tsv.gz"
    with gzip.open(genomes_md5sum, "wt", encoding="utf-8") as f:
        f.write(genomes_md5sum_content)

    # Create and write to `genomes_statistics.tsv.gz`
    genomes_statistics_content = """\
#soft_core=0.95
#duplication_margin=0.05
Genome_name	Contigs	Genes	Fragmented_genes	Families	Families_with_fragments	Families_in_multicopy	Soft_core_families	Soft_core_genes	Exact_core_families	Exact_core_genes	Persistent_genes	Persistent_fragmented_genes	Persistent_families	Persistent_families_with_fragments	Persistent_families_in_multicopy	Shell_genes	Shell_fragmented_genes	Shell_families	Shell_families_with_fragments	Shell_families_in_multicopy	Cloud_genes	Cloud_fragmented_genes	Cloud_families	Cloud_families_with_fragments	Cloud_families_in_multicopy	Completeness	Contamination	Fragmentation	RGPs	Spots	Modules
GenomeA	1	813	4	811	2	2	766	768	766	768	785	4	783	2	2	26	0	26	0	0	2	0	2	0	0	99.87	0.26	0.25	1	0	0
GenomeB	1	811	2	810	1	1	766	767	766	767	784	2	783	1	1	27	0	27	0	0	0	0	0	0	0	99.87	0.13	0.12	1	0	0
"""
    genomes_statistics = pangenome_dir / "genomes_statistics.tsv.gz"
    with gzip.open(genomes_statistics, "wt") as f:
        f.write(genomes_statistics_content)

    return pangenome_dir


def test_create_collection_release(
    session: Session,
    collection: Collection,
    collection_release: CollectionRelease,
    collection_release2: CollectionRelease,
    taxonomy_source: TaxonomySource,
):

    collection_release = create_collection_release(
        collection, collection_release, taxonomy_source, session
    )

    assert collection_release.collection.name == "collection_A"
    assert collection_release.version == "1.0"
    assert collection_release.date == datetime(2021, 1, 1)
    assert collection_release.ppanggolin_version == "3.0.0"
    assert collection_release.pangbank_wf_version == "1.0.0"

    collections = session.exec(select(Collection)).all()
    releases = session.exec(select(CollectionRelease)).all()
    assert len(releases) == 1
    assert len(collections) == 1

    collection_release = create_collection_release(
        collection, collection_release2, taxonomy_source, session
    )

    collections = session.exec(select(Collection)).all()
    releases = session.exec(select(CollectionRelease)).all()
    assert len(releases) == 2
    assert len(collections) == 1

    # add again the same release and collection to check if it is not added again
    collection_release = create_collection_release(
        collection, collection_release2, taxonomy_source, session
    )

    collections = session.exec(select(Collection)).all()
    releases = session.exec(select(CollectionRelease)).all()
    assert len(releases) == 2
    assert len(collections) == 1


def test_create_collection_release_ppanggo_version_mismatch(
    session: Session,
    collection: Collection,
    collection_release: CollectionRelease,
    taxonomy_source: TaxonomySource,
):

    create_collection_release(collection, collection_release, taxonomy_source, session)

    collection_release.ppanggolin_version = "4.0.0"

    with pytest.raises(ValueError):
        collection_release = create_collection_release(
            collection, collection_release, taxonomy_source, session
        )


def test_add_pangenomes_to_db(
    pangenome_dir: Path, collection_release: CollectionRelease, session: Session
):

    pangenome_main_dir = pangenome_dir.parent

    genome_a = Genome(name="GenomeA")
    genome_b = Genome(name="GenomeB")

    genome_name_to_genome = {
        "GenomeA": genome_a,
        "GenomeB": genome_b,
    }

    session.add_all([genome_a, genome_b])
    session.commit()

    pangenomes = add_pangenomes_to_db(
        pangenome_main_dir=pangenome_main_dir,
        collection_release=collection_release,
        genome_name_to_genome=genome_name_to_genome,
        session=session,
    )

    assert len(pangenomes) == 1
    pangenome = pangenomes[0]
    assert pangenome.file_name == "speciesA/pangenome.h5"
    assert len(pangenome.genome_links) == 2

    pangenome_in_DB = session.exec(select(Pangenome)).all()

    assert pangenomes == pangenome_in_DB


def test_delete_full_collection(
    session: Session,
    collection: Collection,
    collection_release: CollectionRelease,
    taxonomy_source: TaxonomySource,
):

    create_collection_release(collection, collection_release, taxonomy_source, session)

    delete_full_collection(session, collection.name)

    collections = session.exec(select(Collection)).all()
    releases = session.exec(select(CollectionRelease)).all()
    assert len(releases) == 0
    assert len(collections) == 0


def test_delete_unexisting_collection(session: Session):

    with pytest.raises(ValueError):
        delete_full_collection(session, "unexisting_collection")


def test_delete_unexisting_collection_release(
    session: Session,
    collection: Collection,
    collection_release: CollectionRelease,
    taxonomy_source: TaxonomySource,
):

    create_collection_release(collection, collection_release, taxonomy_source, session)

    with pytest.raises(ValueError):
        delete_collection_release(session, collection.name, "99999")


def test_delete_collection_release(
    session: Session,
    collection: Collection,
    collection_release: CollectionRelease,
    taxonomy_source: TaxonomySource,
):

    create_collection_release(collection, collection_release, taxonomy_source, session)

    delete_collection_release(session, collection.name, collection_release.version)

    collections = session.exec(select(Collection)).all()
    releases = session.exec(select(CollectionRelease)).all()

    assert len(collections) == 1
    assert len(releases) == 0


def test_print_collections(
    session: Session,
    collection: Collection,
    collection_release: CollectionRelease,
    taxonomy_source: TaxonomySource,
    capsys: pytest.CaptureFixture,  # type: ignore
) -> None:
    """Tests if the collection name appears in console output."""
    create_collection_release(collection, collection_release, taxonomy_source, session)

    with patch("pangbank_api.manage_db.collections.Session", return_value=session):
        print_collections()

    captured = capsys.readouterr()  # type: ignore
    assert "collection_A" in captured.out  # type: ignore
