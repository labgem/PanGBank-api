from pydantic_settings import BaseSettings
from functools import lru_cache
from pathlib import Path


class Settings(BaseSettings):
    pangbank_db_path: Path = Path("database/database.db")
    pangbank_data_dir: Path = Path("data/")
    pangbank_origins: str = (
        "http://localhost:3000"  # list of origins separated by semicolon
    )


@lru_cache
def get_settings():
    return Settings()
