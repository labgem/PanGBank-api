# Functional Tests for PanGBank DB

This directory contains end-to-end functional tests for the `pangbank_db` command-line tool.

## Overview

The functional tests validate the complete workflow of the `pangbank_db` tool from data ingestion to database creation and verification. They create realistic test data structures including:

- Taxonomy files (TSV format with GTDB-style lineages)
- Genome FASTA files
- Genome metadata (CheckM quality metrics)
- Genome status files (GTDB representatives, NCBI references)
- Pangenome data (info.yaml, genomes_statistics.tsv.gz, genomes_md5sum.tsv)

## Running Tests

### 1. As a pytest test

```bash
# Run the functional test with pytest
pytest tests/functional/test_functional_pangbank_db.py -v

# Run all tests including functional
pytest tests/

# Run only functional tests
pytest -m functional

# Run all tests except functional
pytest -m "not functional"
```

### 2. Using the standalone CLI script

The `run_functional_test.py` script provides multiple modes of operation:

#### Full end-to-end test (default)
```bash
python tests/functional/run_functional_test.py
```
This creates temporary test data, runs the test, verifies the database, and cleans up automatically.

#### Keep test data for inspection
```bash
python tests/functional/run_functional_test.py --keep-data
```
Preserves test data in `./functional_test_data/` and database in `./functional_test.db` for manual inspection.

#### Generate test data only
```bash
python tests/functional/run_functional_test.py --generate-only --output ./my_test_data
```
Creates test data files without running the database import. Useful for:
- Inspecting the test data structure
- Manual testing with the `pangbank_db` CLI
- Creating example data for documentation

#### Test with existing data
```bash
python tests/functional/run_functional_test.py --data-dir ./my_test_data --db-path ./my_test.db
```
Runs the test using pre-existing test data and a specific database file.

## Test Data Structure

The generated test data includes:

```
test_data/
├── collection_release_info.json    # Main configuration file
├── taxonomy.tsv                     # GTDB-style taxonomy (3 genomes)
├── genome_list.txt                  # List of genome names
├── genome_metadata.tsv              # Genome metadata and quality metrics
├── mash_sketch.msh                  # Mash sketch file (dummy)
├── genomes/                         # Genome FASTA files
│   ├── GenomeA.fna
│   ├── GenomeB.fna
│   └── GenomeC.fna
├── status/                          # Genome status files
│   ├── gtdb_representatives.txt
│   └── ncbi_references.txt
└── pangenomes/                      # Pangenome data directories
    ├── Escherichia_coli/
    │   ├── info.yaml
    │   ├── genomes_in_pangenome.tsv
    │   ├── genomes_statistics.tsv.gz
    │   ├── genomes_statistics_summary.yaml
    │   ├── genomes_md5sum.tsv
    │   └── pangenome.h5
    └── Salmonella_enterica/
        ├── info.yaml
        ├── genomes_in_pangenome.tsv
        ├── genomes_statistics.tsv.gz
        ├── genomes_statistics_summary.yaml
        ├── genomes_md5sum.tsv
        └── pangenome.h5
```

## What the Tests Verify

The functional tests verify:

1. **Data Import**: All data files are correctly parsed and imported
2. **Database Structure**: Tables are created with correct schema
3. **Data Integrity**: Relationships between tables are maintained
4. **Counts**: Expected number of records are created:
   - 1 collection
   - 1 release
   - 3 genomes (GenomeA, GenomeB, GenomeC)
   - 2 pangenomes (Escherichia_coli, Salmonella_enterica)
   - 2 genome statuses (1 GTDB representative, 1 NCBI reference)

## Using Test Data with pangbank_db CLI

After generating test data, you can use it directly with the `pangbank_db` CLI:

```bash
# Generate test data
python tests/functional/run_functional_test.py --generate-only --output ./my_test

# Set environment variables
export PANGBANK_DB_PATH="./test.db"
export PANGBANK_DATA_DIR="./"

# Run pangbank_db CLI
pangbank_db add-collection-release ./my_test/collection_release_info.json

# Verify the database was created
sqlite3 test.db "SELECT COUNT(*) FROM genome;"
```

## Debugging Failed Tests

If a functional test fails:

1. **Use --keep-data** to inspect the generated test data:
   ```bash
   python tests/functional/run_functional_test.py --keep-data
   ```

2. **Check the database** with SQLite:
   ```bash
   sqlite3 functional_test.db "SELECT name FROM sqlite_master WHERE type='table';"
   ```

3. **Run with verbose pytest output**:
   ```bash
   pytest tests/functional/ -vv --tb=long
   ```

4. **Inspect specific test data files**:
   ```bash
   cat functional_test_data/collection_release_info.json | jq .
   cat functional_test_data/taxonomy.tsv
   ```

## Environment Variables

The functional tests use environment variables to override default configuration:

- `PANGBANK_DB_PATH`: Override the default database path

Example:
```bash
export PANGBANK_DB_PATH=/tmp/my_test.db
pytest tests/functional/
```

## Notes

- The functional tests are marked with `@pytest.mark.functional` and can be selected or excluded with pytest's `-m` flag
- Test data is automatically cleaned up unless `--keep-data` is used
- The tests use realistic but minimal data to ensure fast execution
- Generated pangenome HDF5 files are empty placeholders (not used in current tests)
