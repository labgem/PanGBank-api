from contextlib import asynccontextmanager

try:
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.middleware.gzip import GZipMiddleware
except ImportError:
    raise ImportError(
        "FastAPI is required to run the API server. "
        "Install it with: pip install pangbank-api[fastapi]"
    )

from .database import create_db_and_tables
from .routers import collections, genomes, pangenomes
from .config import get_settings
from importlib.metadata import version
from starlette.middleware.gzip import (
    DEFAULT_EXCLUDED_CONTENT_TYPES,
    GZipMiddleware,
)

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

# Compress larger payloads (e.g. MultiQC HTML reports) for clients that send
# `Accept-Encoding: gzip`.
# app.add_middleware(GZipMiddleware, minimum_size=1024)


app.add_middleware(
    GZipMiddleware,
    minimum_size=1024,
    exclude_content_types=(
        *DEFAULT_EXCLUDED_CONTENT_TYPES,
        "application/x-hdf5",
        "application/octet-stream",
        "model/mesh",
    ),
)


app.include_router(collections.router)
app.include_router(genomes.router)
app.include_router(pangenomes.router)
