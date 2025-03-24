from pydantic_settings import BaseSettings
from functools import lru_cache
from pathlib import Path
from typing import Annotated
from fastapi import Depends


class Settings(BaseSettings):
    pangbank_db_path: Path = Path("database/database.db")
    pangbank_data_dir: Path = Path("data/")


@lru_cache
def get_settings():
    return Settings()


SettingsDep = Annotated[Settings, Depends(get_settings)]
