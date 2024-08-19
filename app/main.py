from fastapi import Depends, FastAPI, HTTPException, Query

from sqlmodel import Session, select

from .database import create_db_and_tables, engine
from .models import Collection, CollectionPublic, CollectionPublicWithReleases, Genome, PangenomePublicWithCollectionRelease, Pangenome, GenomePublic



from contextlib import asynccontextmanager



@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield

app = FastAPI(lifespan=lifespan)

def get_session():
    with Session(engine) as session:
        yield session

@app.get("/collections/", response_model=list[CollectionPublic])
def read_collections(session: Session = Depends(get_session)):

    collections = session.exec(select(Collection)).all()
    return collections

@app.get("/collections/{collection_id}", response_model=CollectionPublicWithReleases)
def get_collection(collection_id, session: Session = Depends(get_session)):

    collection = session.get(Collection, collection_id)
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")
    
    return collection

# @app.get("/genomes/", response_model=list[GenomePublic])
# def read_genomes(offset: int = 0, limit: int = Query(default=100, le=100)):
#     with Session(engine) as session:
#         genomes = session.exec(select(Genome).offset(offset).limit(limit)).all()
#         return genomes
    
@app.get("/genomes/", response_model=list[GenomePublic])
def read_genomes(offset: int = 0, limit: int = Query(default=100, le=100), session: Session = Depends(get_session)):
    genomes = session.exec(select(Genome).offset(offset).limit(limit)).all()
    return genomes

@app.get("/genomes/{genome_id}", response_model=GenomePublic)
def get_genome(genome_id:int, session: Session = Depends(get_session)):

    genome = session.get(Genome, genome_id)
    if not genome:
        raise HTTPException(status_code=404, detail="Genome not found")
    return genome

@app.get("/pangenome/{pangenome_id}", response_model=PangenomePublicWithCollectionRelease)
def get_pangenome(pangenome_id:int, session: Session = Depends(get_session)):

    pangenome = session.get(Pangenome, pangenome_id)
    if not pangenome:
        raise HTTPException(status_code=404, detail="Pangenome not found")
    return pangenome