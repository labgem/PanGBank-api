from sqlmodel import SQLModel, create_engine

from .config import get_settings

settings = get_settings()
sqlite_url = f"sqlite:///{settings.database_path}"

connect_args = {"check_same_thread": False}
engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)
