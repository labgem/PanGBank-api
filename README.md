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

### Manage the database

Interact with the database locally:

   - Add a collection release to the database with 


   ```bash
   pangbank_db add-collection-release <collection_release_json>
   ```
   - List collections

   ```bash
   pangbank_db list-collection
   ```
   
   - Delete a collection release
  
   ```bash
   pangbank_db delete-collection <collection name> --release-version <release version>
   ```

### Docker Compose Setup

1. Build and run the Docker image:
   ```bash
   docker-compose up --build
   ```

<!-- ## API Documentation

FastAPI automatically generates interactive API documentation: -->
<!-- 
- Swagger UI: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- ReDoc: [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc) -->

<!-- ## Environment Variables

Create a `.env` file to store database connection settings and other configurations:

```env
DATABASE_URL=sqlite:///./pangbank.db  # Change for production use
DEBUG=True
``` -->


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
