from contextlib import asynccontextmanager

try:
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
except ImportError:
    raise ImportError(
        "FastAPI is required to run the API server. "
        "Install it with: pip install pangbank-api[fastapi]"
    )

from .database import create_db_and_tables
from .routers import collections, genomes, pangenomes
from .config import get_settings
from importlib.metadata import version

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield

settings = get_settings()

app = FastAPI(
    lifespan=lifespan,
    title="PanGBank API",
    docs_url="/",
    description="API for managing collections pangenomes.",
    version=version("PanGBank-api"),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.pangbank_origins.split(";"),
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


app.include_router(collections.router)
app.include_router(genomes.router)
app.include_router(pangenomes.router)
