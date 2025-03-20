from pydantic_settings import BaseSettings
from functools import lru_cache
from pathlib import Path


class Settings(BaseSettings):
    app_name: str = "PanGBank API"
    database_path: Path = Path("database/database.db")
    data_dir: Path = Path("data/")


@lru_cache
def get_settings():
    return Settings()
