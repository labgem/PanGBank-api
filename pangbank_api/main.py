from contextlib import asynccontextmanager

from fastapi import FastAPI

from .database import create_db_and_tables
from .routers import collections, genomes, pangenomes
from .config import get_settings
from fastapi.middleware.cors import CORSMiddleware

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
