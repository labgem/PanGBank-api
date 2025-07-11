# 🧬 PanGBank API

This repository contains the API used to manage the **PanGBank** database, which stores collections of pangenomes built with [**PPanGGOLiN**](https://github.com/labgem/PPanGGOLiN).

The API is built with [**FastAPI**](https://fastapi.tiangolo.com) and uses [**SQLModel**](https://sqlmodel.tiangolo.com) as its ORM.
It provides a RESTful interface for querying and exploring pangenome collections. Alongside the API, a command-line tool `pangbank_db` is included to manage the database.

## 🚀 Installation

### Local API Setup

1. **Clone the repository**:

   ```bash
   git clone https://github.com/labgem/PanGBank-api.git
   cd PanGBank-api
   ```

2. **Create a virtual environment and install dependencies**:

   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install .
   ```

3. **Run the API in development mode**:

   ```bash
   export PANGBANK_DB_PATH="<path/to/database.sqlite>"
   export PANGBANK_DATA_DIR="<path/to/pangenome_directory>"
   fastapi dev pangbank_api/main.py
   ```

> `PANGBANK_DB_PATH` is the path to your SQLite database file.
> `PANGBANK_DATA_DIR` is the root directory containing your pangenome data and mash files.


## 🛠️ Managing the Database with `pangbank_db`

All CLI commands require the `PANGBANK_DB_PATH` environment variable to be set.

```bash
export PANGBANK_DB_PATH="<path/to/database.sqlite>"
```


### ➕ Add a Collection Release

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


<summary>🧷 JSON Schema Example</summary>



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
  ]
}
```

#### Note
* Paths for `pangenomes_directory` and `mash_sketch` must be **relative to `PANGBANK_DATA_DIR`**.
* Paths for `taxonomy.file`, `genome_sources[*].file`, and `genome_metadata_sources[*].file` must be **absolute file paths**.

</details>


### 📋 List Existing Collections

```bash
pangbank_db list-collection
```

### ❌ Delete a Collection Release

```bash
pangbank_db delete-collection <collection_name> --release-version <version>
```


## 🗃️ Database Migrations with Alembic

We use [Alembic](https://alembic.sqlalchemy.org/) to manage schema changes in the PanGBank database.

### 🔄 Common commands

#### 📐 Create a new migration

Generate a migration after updating your SQLModel models (e.g., adding or changing columns):

```bash
alembic revision --autogenerate -m "Describe your change here"
```

> 🔧 Make sure all your models are imported in `alembic/env.py` before running this.

#### ⬆️ Apply migrations to the database

This applies all pending migrations:

```bash
alembic upgrade head
```

#### ⬇️ Roll back the last migration (use with caution)

If something went wrong, you can revert the last migration:

```bash
alembic downgrade -1
```

Or go back to the base (empty schema):

```bash
alembic downgrade base
```

### 📝 Notes

* The SQLite database path is defined in `config.py` via the `pangbank_db_path` setting (`PANGBANK_DB_PATH` env var).
* Alembic is configured to read this dynamically, so no need to change `alembic.ini`.



## Contributing

1. Fork the repository.
2. Create a feature branch (`git checkout -b feature-name`).
3. Commit your changes (`git commit -m 'Add new feature'`).
4. Push to the branch (`git push origin feature-name`).
5. Open a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For any inquiries or issues, open an issue on the [GitHub repository](https://github.com/labgem/PanGBank-API/issues).
