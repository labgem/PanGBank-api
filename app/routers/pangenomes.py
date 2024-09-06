from fastapi import APIRouter, HTTPException, status, Query
from fastapi.responses import FileResponse
from pathlib import Path

from ..models import PangenomePublic, Pangenome
from ..dependencies import SessionDep
from ..crud import common, pangenomes as pangenomes_crud
from sqlmodel import select

router = APIRouter(
    tags=["pangenomes"],
)

@router.get("/pangenomes/", response_model=list[PangenomePublic])
async def read_pangenomes(session: SessionDep, offset: int = 0, limit: int = Query(default=20, le=100)):

    pangenomes = pangenomes_crud.get_pangenomes(session, offset, limit)

    return pangenomes


@router.get("/pangenomes/{pangenome_id}", response_model=PangenomePublic)
async def get_pangenome(pangenome_id:int, session: SessionDep):

    pangenome = pangenomes_crud.get_pangenome(session, pangenome_id)

    if not pangenome:
        raise HTTPException(status_code=404, detail="Pangenome not found")
    return pangenome


@router.get("/pangenomes/{pangenome_id}/file", response_model=str, response_class=FileResponse)
async def get_pangenome_file(pangenome_id:int, session: SessionDep):
    
    pangenome_file = pangenomes_crud.get_pangenome_file(session, pangenome_id)

    if not pangenome_file:
        raise HTTPException(status_code=404, detail="Pangenome not found")
    

    if not pangenome_file.exists():
        raise HTTPException(status_code=404, detail="Pangenome file does not exists")
    
    return FileResponse(path=pangenome_file.as_posix(), filename="pangenome.h5")
