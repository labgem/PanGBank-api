# PanGBank API

This repository contains the API used to manage the **PanGBank** database, which stores collections of pangenomes built with [**PPanGGOLiN**](https://github.com/labgem/PPanGGOLiN).

The API is built with [**FastAPI**](https://fastapi.tiangolo.com) and uses [**SQLModel**](https://sqlmodel.tiangolo.com) as its ORM.
It provides a RESTful interface for querying and exploring pangenome collections. Alongside the API, a command-line tool `pangbank_db` is included to manage the database.

## Installation

PanGBank-api is organized into two main components:
- **Core package**: Database models, CRUD operations, and CLI tools (`pangbank_db`)
- **API server**: FastAPI-based REST API (optional)

### Option 1: Install Core Package Only

For database management and CLI tools without the API server:

```bash
pip install pangbank-api
```

This installs:
- Database models (`pangbank_api.models`)
- Database utilities (`pangbank_api.database`, `pangbank_api.config`)
- CRUD operations (`pangbank_api.crud`)
- CLI tool `pangbank_db` for database management

### Option 2: Install with FastAPI (Full API Server)

For running the REST API server:

```bash
pip install pangbank-api[fastapi]
```

This additionally installs:
- FastAPI framework
- API routers (`pangbank_api.routers`)
- API server (`pangbank_api.main`)

### Local Development Setup

1. **Clone the repository**:

   ```bash
   git clone https://github.com/labgem/PanGBank-api.git
   cd PanGBank-api
   ```

2. **Create a virtual environment and install with FastAPI**:

   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install .[fastapi]
   ```

3. **Run the API in development mode**:

   ```bash
   export PANGBANK_DB_PATH="<path/to/database.sqlite>"
   export PANGBANK_DATA_DIR="<path/to/pangenome_directory>"
   fastapi dev pangbank_api/main.py
   ```

> `PANGBANK_DB_PATH` is the path to your SQLite database file.
> `PANGBANK_DATA_DIR` is the root directory containing your pangenome data and mash files.


## Managing the Database with `pangbank_db`

All CLI commands require the `PANGBANK_DB_PATH` environment variable to be set.

```bash
export PANGBANK_DB_PATH="<path/to/database.sqlite>"
```


### Add a Collection Release

To add a new collection of pangenomes in the database, use:

```bash
pangbank_db add-collection-release <collection_release.json>
```
> [!NOTE]
> This command requires two environment variables:
>
> ```bash
> export PANGBANK_DB_PATH="<path/to/database.sqlite>"
> export PANGBANK_DATA_DIR="<root/path/serving/pangenomes>"
> ```
  

<details>


<summary>JSON Schema Example</summary>



```jsonc
{
  "collection": {
    "name": "GTDB_all_sampled",
    "description": "GTDB all is a collection of pangenomes made of GTDB species that have at least 15 genomes."
  },
  "release": {
    "version": "1.0.0",
    "ppanggolin_version": "2.2.4",
    "pangbank_wf_version": "0.0.2",
    "pangenomes_directory": "GTDB_refseq/release_v1.0.0/data/pangenomes/", // relative to PANGBANK_DATA_DIR
    "release_note": "",
    "date": "2025-07-10",
    "mash_sketch": "GTDB_refseq/release_v1.0.0/data/mash_sketch/families_persistent_all.msh", // relative to PANGBANK_DATA_DIR
    "mash_version": "2.3"
  },
  "taxonomy": {
    "name": "GTDB",
    "version": "10-RS226",
    "ranks": "Domain; Phylum; Class; Order; Family; Genus; Species",
    "file": "/absolute/path/to/taxonomy.tsv"
  },
  "genome_sources": [
    {
      "name": "RefSeq",
      "file": "/absolute/path/to/genomes.tsv",
      "version": "",
      "description": "",
      "source": "",
      "url": ""
    }
  ],
  "genome_metadata_sources": [
    {
      "name": "GTDB 10-RS226 metadata",
      "description": "Metadata collected from GTDB. Some columns have been filtered out.",
      "url": "https://data.ace.uq.edu.au/public/gtdb/data/releases/release226/226.0/",
      "strain_attribute": "ncbi_strain_identifiers",
      "organism_name_attribute": "ncbi_organism_name",
      "file": "/absolute/path/to/metadata.tsv"
    }
  ],
  "genome_statuses": [
    {
      "status_type": "representative",
      "origin": "GTDB",
      "file": "/absolute/path/to/gtdb_representatives.txt"
    },
    {
      "status_type": "reference",
      "origin": "NCBI_RefSeq",
      "file": "/absolute/path/to/ncbi_references.txt"
    }
  ]
}
```

#### Genome Quality Metrics

The `genome_quality_metrics` field (optional) allows you to load assembly quality metrics and statistics into the Genome table during collection import. The TSV file should have:

- **First column**: `genomes` (genome names matching those in your genome_sources)
- **Other columns**: Any of the following supported quality metrics:
  - `ncbi_genome_category` - NCBI genome category
  - `genome_category` - Custom genome category
  - `checkm2_completeness` - CheckM2 completeness (%)
  - `checkm2_contamination` - CheckM2 contamination (%)
  - `checkm2_model` - CheckM2 model used
  - `checkm_completeness` - CheckM completeness (%)
  - `checkm_contamination` - CheckM contamination (%)
  - `checkm_strain_heterogeneity` - CheckM strain heterogeneity (%)
  - `gc_count` - GC base count
  - `gc_percentage` - GC percentage
  - `genome_size` - Total genome size (bp)
  - `l50_contigs` - L50 contigs statistic
  - `n50_contigs` - N50 contigs statistic

The system automatically handles type conversion (str, int, float) based on the Genome model field types. Only optional fields are updated - required fields like `name` are protected from modification.

**Important**: Quality metrics are immutable once set. If you try to update a genome with different values for existing quality metrics:
- During `add-collection-release`: New values are accepted with a warning (initial import allows overwrites)
- During `add-quality-metrics`: Command **fails with an error** unless `--force` flag is used

This ensures data integrity and prevents accidental corruption of quality metric data.

#### Note
* Paths for `pangenomes_directory` and `mash_sketch` must be **relative to `PANGBANK_DATA_DIR`**.
* Paths for `taxonomy.file`, `genome_sources[*].file`, `genome_quality_metrics.file`, and `genome_statuses[*].file` must be **absolute file paths**.
* `genome_quality_metrics` and `genome_statuses` are optional.
* Each genome status file should contain one genome name per line.

</details>


### List Existing Collections

```bash
pangbank_db list-collections
```

### Delete a Collection Release

```bash
pangbank_db delete-collection <collection_name> --release-version <version>
```

### Add Genome Statuses to an Existing Release

Add genome status information (representative, reference, type strain, etc.) to an existing collection release without re-importing the entire collection:

```bash
pangbank_db add-genome-statuses \
  --collection-name <collection_name> \
  --release-version <release_version> \
  --status-type <status_type> \
  --origin <origin> \
  --file <file>
```

**Example:**

```bash
pangbank_db add-genome-statuses \
  --collection-name "GTDB_all_sampled" \
  --release-version "1.0.0" \
  --status-type "representative" \
  --origin "GTDB" \
  --file /path/to/gtdb_representatives.txt
```



This command is useful for:
- Adding genome statuses to releases that were imported without them
- Updating status information when new representative/reference genomes are announced
- Adding multiple status types incrementally (e.g., first representatives, then type strains)

The file should contain one genome name per line. Duplicate statuses are automatically skipped.


### Add Quality Metrics to Existing Genomes

Add or update genome quality metrics (CheckM completeness, contamination, genome size, etc.) for genomes already in the database:

```bash
pangbank_db add-quality-metrics <genome_quality_metrics.tsv>

# Force overwrite existing values (with warnings)
pangbank_db add-quality-metrics <genome_quality_metrics.tsv> --force
```

**Example:**

```bash
# Add new quality metrics (fails if trying to change existing values)
pangbank_db add-quality-metrics /path/to/gtdb_genome_quality_metrics.tsv

# Intentionally overwrite existing metrics (logs warnings)
pangbank_db add-quality-metrics /path/to/updated_metrics.tsv --force
```

The TSV file should have:
- A `genomes` column with genome names
- Quality metric columns matching the Genome model fields (e.g., `checkm2_completeness`, `checkm2_contamination`, `genome_size`, `gc_percentage`)

**Important Notes:**
- Only columns that match optional Genome fields will be imported; unknown columns are automatically filtered out
- Quality metrics are **immutable by default** - attempting to change existing values **raises an error**
- Use `--force` flag to intentionally overwrite existing values (warnings will be logged for each change)
- If a genome already has a value for a field:
  - Identical values are skipped (idempotent operation)
  - Different values **raise an error** unless `--force` is used
- Only fields with `None` (no existing value) are updated without restriction

This command is useful for:
- Adding quality metrics after initial data import
- Importing metrics for newly added genomes
- Safely re-running imports without data corruption risk

**Example TSV format:**
```tsv
genomes	checkm2_completeness	checkm2_contamination	genome_size	gc_percentage
GenomeA	98.5	0.2	5000000	45.5
GenomeB	95.0	1.5	4500000	42.0
```


## Database Migrations with Alembic

We use [Alembic](https://alembic.sqlalchemy.org/) to manage schema changes in the PanGBank database.


#### Create a new migration

Generate a migration after updating your SQLModel models (e.g., adding or changing columns):

```bash
alembic revision --autogenerate -m "Describe your change here"
```

#### Apply migrations to the database

This applies all pending migrations:

```bash
alembic upgrade head
```

#### Roll back the last migration (use with caution)

If something went wrong, you can revert the last migration:

```bash
alembic downgrade -1
```

Or go back to the base (empty schema):

```bash
alembic downgrade base
```

> [!NOTE]
> * The SQLite database path is defined in `config.py` via the `pangbank_db_path` setting (`PANGBANK_DB_PATH` env var).
>* Alembic is configured to read this dynamically, so no need to change `alembic.ini`.



## Contributing

1. Fork the repository.
2. Create a feature branch (`git checkout -b feature-name`).
3. Commit your changes (`git commit -m 'Add new feature'`).
4. Push to the branch (`git push origin feature-name`).
5. Open a pull request.


## Contact

For any inquiries or issues, open an issue on the [GitHub repository](https://github.com/labgem/PanGBank-API/issues).
