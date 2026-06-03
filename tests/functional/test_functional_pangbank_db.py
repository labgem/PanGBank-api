"""
Functional end-to-end test for pangbank_db.

This test can be run:
1. As a pytest test: pytest tests/functional/test_functional_pangbank_db.py
2. As a standalone script: python tests/functional/test_functional_pangbank_db.py
3. Via the separate CLI script: python tests/functional/run_functional_test.py
"""

from __future__ import annotations

import gzip
import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from sqlmodel import select

from pangbank_api.models import Collection, CollectionRelease, Genome, Pangenome


def create_test_taxonomy_file(test_dir: Path) -> Path:
    """Create a minimal taxonomy TSV file for testing."""
    taxonomy_file = test_dir / "taxonomy.tsv"
    
    # Format: genome_name\ttaxonomy_string (semicolon-separated)
    taxonomy_content = """GenomeA\td__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Enterobacteriaceae;g__Escherichia;s__Escherichia_coli
GenomeB\td__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Enterobacteriaceae;g__Escherichia;s__Escherichia_coli
GenomeC\td__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;o__Enterobacterales;f__Enterobacteriaceae;g__Salmonella;s__Salmonella_enterica
"""
    
    taxonomy_file.write_text(taxonomy_content)
    return taxonomy_file


def create_test_genome_files(test_dir: Path) -> tuple[Path, Path]:
    """Create minimal genome FASTA files and a genome list file for testing."""
    genomes_dir = test_dir / "genomes"
    genomes_dir.mkdir(parents=True, exist_ok=True)
    
    genome_files:list[Path] = []
    genome_names = ["GenomeA", "GenomeB", "GenomeC"]
    
    # Create FASTA files
    for genome_name in genome_names:
        genome_file = genomes_dir / f"{genome_name}.fna"
        genome_content = """>seq1
ATCGATCGATCGATCGATCG
>seq2
GCTAGCTAGCTAGCTAGCTA
"""
        genome_file.write_text(genome_content)
        genome_files.append(genome_file)
    
    # Create genome list file (required by genome source)
    genome_list_file = test_dir / "genome_list.txt"
    genome_list_file.write_text("\n".join(genome_names) + "\n")
    
    return genomes_dir, genome_list_file


def create_test_metadata_file(test_dir: Path) -> Path:
    """Create a minimal genome quality metrics TSV file for testing."""
    metadata_file = test_dir / "genome_quality_metrics.tsv"

    # First column must be "genomes"
    # Include various quality metrics columns that map to Genome table
    metadata_content = """genomes\tcheckm2_completeness\tcheckm2_contamination\tgenome_size\tgc_percentage
GenomeA\t98.5\t0.5\t5000000\t50.5\tGCA_123456
GenomeB\t99.2\t0.3\t4800000\t51.2\tGCA_789012
GenomeC\t97.8\t1.2\t5200000\t49.8\tGCA_345678
"""

    metadata_file.write_text(metadata_content)
    return metadata_file


def create_test_genome_status_files(test_dir: Path) -> dict[str, Path]:
    """Create genome status files (representative/reference genome lists)."""
    status_dir = test_dir / "status"
    status_dir.mkdir(parents=True, exist_ok=True)
    
    # GTDB representatives
    gtdb_file = status_dir / "gtdb_representatives.txt"
    gtdb_file.write_text("GenomeA\n")
    
    # NCBI reference genomes
    ncbi_file = status_dir / "ncbi_references.txt"
    ncbi_file.write_text("GenomeB\n")
    
    return {
        "gtdb": gtdb_file,
        "ncbi": ncbi_file,
    }


def create_test_pangenome_files(test_dir: Path) -> Path:
    """Create minimal pangenome HDF5 files with accompanying metadata."""
    pangenomes_dir = test_dir / "pangenomes"
    
    # Create species directory
    species_dir = pangenomes_dir / "Escherichia_coli"
    species_dir.mkdir(parents=True, exist_ok=True)
    
    # Create info.yaml
    info_yaml = species_dir / "info.yaml"
    info_yaml_content = """Content:
    Genes: 10000
    Genomes: 2
    Families: 500
    Edges: 450
    Number_of_partitions: 4
    RGP: 10
    Spots: 5
    Modules:
        Number_of_modules: 15
        Families_in_Modules: 200
    Genomes_fluidity:
        all: 0.25
        shell: 0.35
        cloud: 0.45
        accessory: 0.30
    Persistent:
        Family_count: 400
        min_genomes_frequency: 0.8
        max_genomes_frequency: 1.0
        sd_genomes_frequency: 0.05
        mean_genomes_frequency: 0.95
    Shell:
        Family_count: 75
        min_genomes_frequency: 0.2
        max_genomes_frequency: 0.8
        sd_genomes_frequency: 0.15
        mean_genomes_frequency: 0.5
    Cloud:
        Family_count: 25
        min_genomes_frequency: 0.0
        max_genomes_frequency: 0.2
        sd_genomes_frequency: 0.05
        mean_genomes_frequency: 0.1
Parameters:
    PPanGGOLiN_version: 2.0.0
"""
    info_yaml.write_text(info_yaml_content)
    
    # Create genomes_in_pangenome.tsv
    genomes_tsv = species_dir / "genomes_in_pangenome.tsv"
    genomes_tsv_content = """Genome_name\tContigs\tGenes\tFragmented_genes\tFamilies\tFamilies_with_fragments\tFamilies_in_multicopy\tSoft_core_families\tSoft_core_genes\tExact_core_families\tExact_core_genes\tPersistent_genes\tPersistent_fragmented_genes\tPersistent_families\tPersistent_families_with_fragments\tPersistent_families_in_multicopy\tShell_genes\tShell_fragmented_genes\tShell_families\tShell_families_with_fragments\tShell_families_in_multicopy\tCloud_genes\tCloud_fragmented_genes\tCloud_families\tCloud_families_with_fragments\tCloud_families_in_multicopy\tCompleteness\tContamination\tFragmentation\tRGPs\tSpots\tModules
GenomeA\t50\t5000\t25\t450\t5\t10\t400\t4000\t380\t3800\t4500\t20\t425\t4\t8\t350\t3\t50\t1\t2\t150\t2\t25\t0\t0\t98.5\t0.5\t0.02\t5\t3\t8
GenomeB\t45\t5000\t20\t455\t4\t9\t405\t4050\t385\t3850\t4550\t18\t430\t3\t7\t345\t2\t48\t1\t2\t155\t0\t27\t0\t0\t99.2\t0.3\t0.015\t5\t2\t7
"""
    genomes_tsv.write_text(genomes_tsv_content)
    
    # Create genomes_statistics_summary.yaml
    genomes_stats_yaml = species_dir / "genomes_statistics_summary.yaml"
    genomes_stats_content = """Completeness:
    mean: 98.85
Contamination:
    mean: 0.4
Fragmentation:
    mean: 0.0175
Exact_core_families:
    mean: 382.5
Soft_core_families:
    mean: 402.5
Persistent_families:
    mean: 427.5
Shell_families:
    mean: 49.0
Cloud_families:
    mean: 26.0
"""
    genomes_stats_yaml.write_text(genomes_stats_content)
    
    # Create genomes_md5sum.tsv
    md5sum_tsv = species_dir / "genomes_md5sum.tsv"
    md5sum_content = """name\tmd5_sum\tfile_name
GenomeA\ta1b2c3d4e5f6g7h8i9j0\tGenomeA.fna
GenomeB\tb2c3d4e5f6g7h8i9j0k1\tGenomeB.fna
"""
    md5sum_tsv.write_text(md5sum_content)
    
    # Create genomes_statistics.tsv.gz (gzipped version)
    genomes_stats_tsv_gz = species_dir / "genomes_statistics.tsv.gz"
    with gzip.open(genomes_stats_tsv_gz, "wt") as f:
        f.write(genomes_tsv_content)
    
    # Create a minimal HDF5 file (empty but valid)
    pangenome_h5 = species_dir / "pangenome.h5"
    # We'll create an empty file - real implementation would have HDF5 structure
    pangenome_h5.write_bytes(b"")
    
    # Create second species
    species_dir2 = pangenomes_dir / "Salmonella_enterica"
    species_dir2.mkdir(parents=True, exist_ok=True)
    
    info_yaml2 = species_dir2 / "info.yaml"
    info_yaml2_content = """Content:
    Genes: 5000
    Genomes: 1
    Families: 300
    Edges: 250
    Number_of_partitions: 3
    RGP: 3
    Spots: 2
    Modules:
        Number_of_modules: 10
        Families_in_Modules: 150
    Genomes_fluidity:
        all: 0.0
        shell: 0.0
        cloud: 0.0
        accessory: 0.0
    Persistent:
        Family_count: 280
        min_genomes_frequency: 1.0
        max_genomes_frequency: 1.0
        sd_genomes_frequency: 0.0
        mean_genomes_frequency: 1.0
    Shell:
        Family_count: 15
        min_genomes_frequency: 0.0
        max_genomes_frequency: 0.0
        sd_genomes_frequency: 0.0
        mean_genomes_frequency: 0.0
    Cloud:
        Family_count: 5
        min_genomes_frequency: 0.0
        max_genomes_frequency: 0.0
        sd_genomes_frequency: 0.0
        mean_genomes_frequency: 0.0
Parameters:
    PPanGGOLiN_version: 2.0.0
"""
    info_yaml2.write_text(info_yaml2_content)
    
    genomes_tsv2 = species_dir2 / "genomes_in_pangenome.tsv"
    genomes_tsv2_content = """Genome_name\tContigs\tGenes\tFragmented_genes\tFamilies\tFamilies_with_fragments\tFamilies_in_multicopy\tSoft_core_families\tSoft_core_genes\tExact_core_families\tExact_core_genes\tPersistent_genes\tPersistent_fragmented_genes\tPersistent_families\tPersistent_families_with_fragments\tPersistent_families_in_multicopy\tShell_genes\tShell_fragmented_genes\tShell_families\tShell_families_with_fragments\tShell_families_in_multicopy\tCloud_genes\tCloud_fragmented_genes\tCloud_families\tCloud_families_with_fragments\tCloud_families_in_multicopy\tCompleteness\tContamination\tFragmentation\tRGPs\tSpots\tModules
GenomeC\t60\t5000\t30\t300\t3\t5\t280\t2800\t280\t2800\t4700\t25\t290\t2\t4\t100\t3\t15\t1\t1\t200\t2\t5\t0\t0\t97.8\t1.2\t0.03\t3\t2\t10
"""
    genomes_tsv2.write_text(genomes_tsv2_content)
    
    # Create genomes_statistics_summary.yaml for second species
    genomes_stats_yaml2 = species_dir2 / "genomes_statistics_summary.yaml"
    genomes_stats_content2 = """Completeness:
    mean: 97.8
Contamination:
    mean: 1.2
Fragmentation:
    mean: 0.03
Exact_core_families:
    mean: 280.0
Soft_core_families:
    mean: 280.0
Persistent_families:
    mean: 290.0
Shell_families:
    mean: 15.0
Cloud_families:
    mean: 5.0
"""
    genomes_stats_yaml2.write_text(genomes_stats_content2)
    
    # Create genomes_md5sum.tsv for second species
    md5sum_tsv2 = species_dir2 / "genomes_md5sum.tsv"
    md5sum_content2 = """name\tmd5_sum\tfile_name
GenomeC\tc3d4e5f6g7h8i9j0k1l2\tGenomeC.fna
"""
    md5sum_tsv2.write_text(md5sum_content2)
    
    # Create genomes_statistics.tsv.gz (gzipped version)
    genomes_stats_tsv_gz2 = species_dir2 / "genomes_statistics.tsv.gz"
    with gzip.open(genomes_stats_tsv_gz2, "wt") as f:
        f.write(genomes_tsv2_content)
    
    pangenome_h5_2 = species_dir2 / "pangenome.h5"
    pangenome_h5_2.write_bytes(b"")
    
    return pangenomes_dir


def create_collection_release_json(
    test_dir: Path,
    taxonomy_file: Path,
    metadata_file: Path,
    genome_list_file: Path,
    status_files: dict[str, Path],
    mash_sketch_file: Path,
) -> Path:
    """Create the collection release JSON configuration file."""

    json_file = test_dir / "collection_release_info.json"

    # Use absolute paths to avoid path resolution issues
    config: dict[str, Any] = {
        "collection": {
            "name": "test_collection",
            "description": "Functional test collection",
        },
        "release": {
            "version": "1.0.0",
            "ppanggolin_version": "2.0.0",
            "pangbank_wf_version": "1.0.0",
            "release_note": "Functional test release",
            "mash_version": "2.3",
            "date": datetime.now().isoformat(),
            "pangenomes_directory": str(test_dir / "pangenomes"),
            "mash_sketch": str(mash_sketch_file),
        },
        "taxonomy": {
            "name": "GTDB",
            "version": "R220",
            "ranks": "Domain;Phylum;Class;Order;Family;Genus;Species",
            "description": "Test taxonomy",
            "source": "test",
            "url": "https://test.example.com",
            "file": str(taxonomy_file),
        },
        "genome_sources": [
            {
                "name": "TestGenomes",
                "version": "1.0",
                "description": "Test genome source",
                "source": "test",
                "url": "https://test.example.com",
                "file": str(genome_list_file),
            }
        ],
        "genome_quality_metrics": {"file": str(metadata_file)},
        "genome_statuses": [
            {
                "status_type": "representative",
                "origin": "GTDB_R220",
                "file": str(status_files["gtdb"]),
            },
            {
                "status_type": "reference",
                "origin": "NCBI_RefSeq",
                "file": str(status_files["ncbi"]),
            },
        ],
    }

    json_file.write_text(json.dumps(config, indent=2))
    return json_file


def setup_functional_test_data(test_dir: Path) -> Path:
    """
    Set up all test data files needed for functional testing.
    
    Returns:
        Path to the collection release JSON file
    """
    # Create all test data files
    taxonomy_file = create_test_taxonomy_file(test_dir)
    _genomes_dir, genome_list_file = create_test_genome_files(test_dir)
    metadata_file = create_test_metadata_file(test_dir)
    status_files = create_test_genome_status_files(test_dir)
    _pangenomes_dir = create_test_pangenome_files(test_dir)
    
    # Create a dummy mash sketch file
    mash_sketch_file = test_dir / "mash_sketch.msh"
    mash_sketch_file.write_bytes(b"dummy mash sketch content")
    
    # Create the collection release configuration JSON
    json_file = create_collection_release_json(
        test_dir,
        taxonomy_file,
        metadata_file,
        genome_list_file,
        status_files,
        mash_sketch_file,
    )
    
    return json_file


def verify_database_content(db_path: Path) -> dict[str, Any]:
    """
    Verify that the database was populated correctly.
    
    Returns:
        Dictionary with counts and verification results
    """
    from sqlmodel import create_engine, Session

    # Create engine for the test database
    sqlite_url = f"sqlite:///{db_path}"
    test_engine = create_engine(sqlite_url, echo=False, connect_args={"check_same_thread": False})

    with Session(test_engine) as session:
        # Count records
        collections = session.exec(select(Collection)).all()
        releases = session.exec(select(CollectionRelease)).all()
        genomes = session.exec(select(Genome)).all()
        pangenomes = session.exec(select(Pangenome)).all()

        results: dict[str, Any] = {
            "collections_count": len(collections),
            "releases_count": len(releases),
            "genomes_count": len(genomes),
            "pangenomes_count": len(pangenomes),
            "success": True,
        }

        # Verify expected values
        if len(collections) != 1:
            results["success"] = False
            results["error"] = f"Expected 1 collection, got {len(collections)}"

        if len(releases) != 1:
            results["success"] = False
            results["error"] = f"Expected 1 release, got {len(releases)}"

        if len(genomes) != 3:
            results["success"] = False
            results["error"] = f"Expected 3 genomes, got {len(genomes)}"

        if len(pangenomes) != 2:
            results["success"] = False
            results["error"] = f"Expected 2 pangenomes, got {len(pangenomes)}"

        # Check genome statuses
        if results["success"] and releases:
            release = releases[0]
            genome_statuses = release.genome_statuses
            if len(genome_statuses) != 2:
                results["success"] = False
                results["error"] = f"Expected 2 genome statuses, got {len(genome_statuses)}"

        # Check that genome quality metrics were loaded
        if results["success"] and genomes:
            genomes_with_metrics = [
                g for g in genomes if g.checkm2_completeness is not None
            ]
            if len(genomes_with_metrics) != 3:
                results["success"] = False
                results["error"] = (
                    f"Expected 3 genomes with quality metrics, got {len(genomes_with_metrics)}"
                )

            # Verify specific values
            genome_a = next((g for g in genomes if g.name == "GenomeA"), None)
            if genome_a:
                if genome_a.checkm2_completeness != 98.5:
                    results["success"] = False
                    results["error"] = (
                        f"Expected GenomeA completeness 98.5, got {genome_a.checkm2_completeness}"
                    )
                if genome_a.genome_size != 5000000:
                    results["success"] = False
                    results["error"] = (
                        f"Expected GenomeA size 5000000, got {genome_a.genome_size}"
                    )

        return results


@pytest.mark.functional
def test_functional_pangbank_db_end_to_end(tmp_path: Path):
    """
    Functional end-to-end test for pangbank_db.
    
    Tests the complete workflow:
    1. Create test data files
    2. Run add_collection_release command
    3. Verify database content
    """
    import os
    
    # Create test data directory
    test_data_dir = tmp_path / "test_data"
    test_data_dir.mkdir(parents=True, exist_ok=True)
    
    # Create test database
    test_db_path = tmp_path / "test_pangbank.db"
    
    # Ensure parent directory exists
    test_db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Set up test data
    json_file = setup_functional_test_data(test_data_dir)
    
    # Override database path using environment variable
    original_db_env = os.environ.get("PANGBANK_DB_PATH")
    os.environ["PANGBANK_DB_PATH"] = str(test_db_path)
    
    try:
        # Clear import cache to reload config with new env var
        import sys
        if "pangbank_api.config" in sys.modules:
            del sys.modules["pangbank_api.config"]
        if "pangbank_api.database" in sys.modules:
            del sys.modules["pangbank_api.database"]
        if "pangbank_api.manage_db.pangbank_db" in sys.modules:
            del sys.modules["pangbank_api.manage_db.pangbank_db"]
        
        # Import after setting environment variable
        from pangbank_api.manage_db.pangbank_db import add_collection_release
        
        # Run the functional test
        add_collection_release(
            collection_release_json=json_file,
            pangbank_data_dir=test_data_dir,
        )
        
        # Verify results
        results = verify_database_content(test_db_path)
        
        assert results["success"], results.get("error", "Unknown error")
        assert results["collections_count"] == 1
        assert results["releases_count"] == 1
        assert results["genomes_count"] == 3
        assert results["pangenomes_count"] == 2
        
    finally:
        # Restore original database path
        if original_db_env is not None:
            os.environ["PANGBANK_DB_PATH"] = original_db_env
        elif "PANGBANK_DB_PATH" in os.environ:
            del os.environ["PANGBANK_DB_PATH"]


def run_standalone_test():
    """
    Run the functional test as a standalone script.
    
    Usage:
        python tests/functional/test_functional_pangbank_db.py
    """
    print("=" * 70)
    print("Running Functional Test for pangbank_db")
    print("=" * 70)
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        try:
            # Run the test
            test_functional_pangbank_db_end_to_end(tmp_path)
            
            print("\n[SUCCESS] Functional test PASSED!")
            print("\nTest verified:")
            print("  - Collection created successfully")
            print("  - Release added successfully")
            print("  - 3 genomes imported")
            print("  - 2 pangenomes imported")
            print("  - Genome statuses recorded")
            
            return 0
            
        except AssertionError as e:
            print(f"\n[FAILED] Functional test FAILED: {e}")
            return 1
        except Exception as e:
            print(f"\n[ERROR] Functional test ERROR: {e}")
            import traceback
            traceback.print_exc()
            return 1


if __name__ == "__main__":
    import sys
    sys.exit(run_standalone_test())
