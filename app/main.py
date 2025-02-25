from contextlib import asynccontextmanager

from fastapi import FastAPI

from .database import create_db_and_tables
from .routers import collections, genomes, pangenomes


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield


app = FastAPI(
    lifespan=lifespan,
    title="PanGBank API",
    docs_url="/",
    description="API for managing collections pangenomes.",
)

app.include_router(collections.router)
app.include_router(genomes.router)
app.include_router(pangenomes.router)
