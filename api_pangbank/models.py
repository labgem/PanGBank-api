
from sqlmodel import Field, Relationship, SQLModel

from datetime import datetime
from pathlib import Path

# class GenomeCollectionReleaseLink(SQLModel, table=True):
#     genome_id: int | None = Field(default=None, foreign_key="genome.id", primary_key=True)
#     collection_release_id: int | None = Field(default=None, foreign_key="collectionrelease.id", primary_key=True)


class GenomeTaxonLink(SQLModel, table=True):
    genome_id: int | None = Field(default=None, foreign_key="genome.id", primary_key=True)
    taxon_id: int | None = Field(default=None, foreign_key="taxon.id", primary_key=True)


class GenomePangenomeLink(SQLModel, table=True):
    genome_id: int | None = Field(default=None, foreign_key="genome.id", primary_key=True)
    pangenome_id: int | None = Field(default=None, foreign_key="pangenome.id", primary_key=True)

    pangenome: "Pangenome" = Relationship(back_populates="genome_links")
    genome: "Genome" = Relationship(back_populates="pangenome_links")

    genome_file_md5sum : str 
    genome_file_name : str


class Collection(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name : str = Field(unique=True)
    description : str | None = None
    
    collection_releases : list["CollectionRelease"] = Relationship(back_populates="collection")


class TaxonomySource(SQLModel, table=True):

    id: int | None = Field(default=None, primary_key=True)
    name : str = Field(unique=True)
    ranks : str
    version : str | None
    description : str | None = None
    source : str | None = None
    url : str | None = None

    collection_releases : list["CollectionRelease"] = Relationship(back_populates="taxonomy_source")

    taxa: list["Taxon"] = Relationship(back_populates="taxonomy_source")

class GenomeSource(SQLModel, table=True):

    id: int | None = Field(default=None, primary_key=True)
    name : str = Field(unique=True)
    version : str | None
    description : str | None = None
    source : str | None = None
    url : str | None = None

    genomes: list["Genome"] = Relationship(back_populates="genome_source")




class CollectionRelease(SQLModel, table=True):

    id: int | None = Field(default=None, primary_key=True)
    version : str

    ppanggolin_version : str
    pangbank_wf_version : str
    pangenomes_directory : str
    date: datetime = Field(default_factory=datetime.now, nullable=False)

    description : str | None = None
    state : str | None = None   
    
    collection_id : int | None = Field(default=None, foreign_key="collection.id")
    collection: Collection = Relationship(back_populates="collection_releases")

    taxonomy_source_id : int | None = Field(default=None, foreign_key="taxonomysource.id")
    taxonomy_source : TaxonomySource = Relationship(back_populates="collection_releases")

    pangenomes: list["Pangenome"] = Relationship(back_populates="collection_release")


    # genomes: list["Genome"] = Relationship(back_populates="collection_releases", link_model=GenomeCollectionReleaseLink)



class Taxon(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)

    name : str
    rank : str
    depth : int 

    taxid : int | None = None

    taxonomy_source_id : int | None = Field(default=None, foreign_key="taxonomysource.id")
    taxonomy_source : TaxonomySource = Relationship(back_populates="taxa")

    genomes: list["Genome"] = Relationship(back_populates="taxa", link_model=GenomeTaxonLink)




class Genome(SQLModel, table=True):

    id: int | None = Field(default=None, primary_key=True)
    name: str
    # version : str | None = None
    # collection_releases : list[CollectionRelease] = Relationship(back_populates="genomes", link_model=GenomeCollectionReleaseLink)

    taxa : list[Taxon] =  Relationship(back_populates="genomes", link_model=GenomeTaxonLink)

    pangenome_links: list[GenomePangenomeLink] = Relationship(back_populates="genome")

    # pangenomes : list["Pangenome"] = Relationship(back_populates="genomes", link_model=GenomePangenomeLink)

    genome_source_id : int | None = Field(default=None, foreign_key="genomesource.id")
    
    genome_source: GenomeSource = Relationship(back_populates="genomes")


class Pangenome(SQLModel, table=True):

    id: int | None = Field(default=None, primary_key=True)

    collection_release_id: int | None = Field(default=None, foreign_key="collectionrelease.id")
    collection_release: CollectionRelease = Relationship(back_populates="pangenomes")
    
    file_name: str

    genome_links: list[GenomePangenomeLink] = Relationship(back_populates="pangenome")
    # genomes : list[Genome] = Relationship(back_populates="pangenomes", link_model=GenomePangenomeLink)

    annotation_source : str | None = None 

