from fastapi import APIRouter, HTTPException

from ..models import PangenomePublicWithCollectionRelease, Pangenome
from ..dependencies import SessionDep
from sqlmodel import select

router = APIRouter(
    tags=["pangenomes"],
)

@router.get("/pangenomes/", response_model=list[PangenomePublicWithCollectionRelease])
def read_pangenomes(session: SessionDep):

    pangenomes = session.exec(select(Pangenome)).all()
    return pangenomes


@router.get("/pangenomes/{pangenome_id}", response_model=PangenomePublicWithCollectionRelease)
def get_pangenome(pangenome_id:int, session: SessionDep):

    pangenome = session.get(Pangenome, pangenome_id)
    if not pangenome:
        raise HTTPException(status_code=404, detail="Pangenome not found")
    return pangenome


