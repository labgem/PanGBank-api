from collections.abc import Generator
from typing import Annotated

try:
    from fastapi import Depends
except ImportError:
    raise ImportError(
        "FastAPI is required for API dependencies. "
        "Install it with: pip install pangbank-api[fastapi]"
    )

from sqlmodel import Session

from .database import engine
from .config import Settings, get_settings


def get_session() -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
