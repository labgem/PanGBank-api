from fastapi import FastAPI

from .database import create_db_and_tables
from .routers import collections, genomes, pangenomes

from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield

app = FastAPI(lifespan=lifespan)    

app.include_router(collections.router)
app.include_router(genomes.router)
app.include_router(pangenomes.router)

some_file_path = "/home/jmainguy/Codes/api_pangbank/cmd.sh"

