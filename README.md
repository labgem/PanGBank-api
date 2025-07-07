# PanGBank API

PanGBank is an API designed to manage a database of pangenomes. Built with **FastAPI** and using **SQLModel** as the ORM. The API can be deployed using **Docker**.

## Installation

### Local Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/labgem/PanGBank-api.git
   cd PanGBank-api
   ```

2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install .
   ```

3. Run the API in development mode:
   ```bash
   export PANGBANK_DB_PATH="<Path to the SQLlite database>"
   export PANGBANK_DATA_DIR="<Path to the Directory containing Pangenomes>"
   fastapi dev pangbank_api/main.py
   ```

## Manage the database

Interact with the database locally. You need to define the environement variable `PANGBANK_DB_PATH`.

### Add a collection release to the database with 

```bash
pangbank_db add-collection-release <collection_release_json>
```
### List collections

```bash
pangbank_db list-collection
```

### Delete a collection release

```bash
pangbank_db delete-collection <collection name> --release-version <release version>
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
