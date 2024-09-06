from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import FileResponse

from ..models import PangenomePublic
from ..dependencies import SessionDep
from app.crud import pangenomes as pangenomes_crud
from app.crud.common import FilterPangenome


router = APIRouter(
    tags=["pangenomes"],
)

@router.get("/pangenomes/", response_model=list[PangenomePublic])
async def get_pangenomes(session: SessionDep, filter_params: FilterPangenome = Depends()):

    pangenomes = pangenomes_crud.get_pangenomes(session, filter_params)

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



@router.get("/pangenomes/count/", response_model=int)
async def get_pangenome_count(session: SessionDep, filter_params: FilterPangenome = Depends()):

    pangenomes = pangenomes_crud.get_pangenomes(session, filter_params)

    return len(pangenomes)

