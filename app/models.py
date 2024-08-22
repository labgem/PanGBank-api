
from sqlmodel import Field, Relationship, SQLModel
from pydantic import BaseModel
from datetime import datetime

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

class CollectionBase(SQLModel):
    name : str = Field(unique=True)
    description : str | None = None
    
class Collection(CollectionBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

    collection_releases : list["CollectionRelease"] = Relationship(back_populates="collection")

class CollectionPublic(CollectionBase):
    id: int

class CollectionPublicWithReleases(CollectionPublic):
    
    collection_releases : list["CollectionReleasePublic"]

class TaxonomySourceBase(SQLModel):
    name: str
    ranks: str
    version: str | None = None
    description: str | None = None
    source: str | None = None
    url: str | None = None

class TaxonomySource(TaxonomySourceBase, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(unique=True)  # Ensuring name uniqueness at the model level

    collection_releases: list["CollectionRelease"] = Relationship(back_populates="taxonomy_source")
    taxa: list["Taxon"] = Relationship(back_populates="taxonomy_source")

class TaxonomySourcePublic(TaxonomySourceBase):
    id: int


class GenomeSourceBase(SQLModel):
    name: str
    version: str | None = None
    description: str | None = None
    source: str | None = None
    url: str | None = None

class GenomeSource(GenomeSourceBase, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(unique=True)  # Ensuring name uniqueness at the model level

    genomes: list["Genome"] = Relationship(back_populates="genome_source")

class GenomeSourcePublic(GenomeSourceBase):
    id: int

class CollectionReleaseBase(SQLModel):
    version: str
    ppanggolin_version: str
    pangbank_wf_version: str
    pangenomes_directory: str
    date: datetime = Field(default_factory=datetime.now, nullable=False)
    description: str | None = None
    state: str | None = None

    collection_id: int | None = Field(default=None, foreign_key="collection.id")

    taxonomy_source_id: int | None = Field(default=None, foreign_key="taxonomysource.id")


class CollectionRelease(CollectionReleaseBase, table=True):
    id: int | None = Field(default=None, primary_key=True)
    
    collection: "Collection" = Relationship(back_populates="collection_releases")

    taxonomy_source: "TaxonomySource" = Relationship(back_populates="collection_releases")

    pangenomes: list["Pangenome"] = Relationship(back_populates="collection_release")

class CollectionReleasePublic(CollectionReleaseBase):
    id: int


class TaxonBase(SQLModel):

    name : str
    rank : str
    depth : int 

    taxid : int | None = None

    taxonomy_source_id : int | None = Field(default=None, foreign_key="taxonomysource.id")

class Taxon(TaxonBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

    taxonomy_source : TaxonomySource = Relationship(back_populates="taxa")

    genomes: list["Genome"] = Relationship(back_populates="taxa", link_model=GenomeTaxonLink)


class TaxonPublic(TaxonBase):
    id : int



class GenomeBase(SQLModel):

    name : str
    genome_source_id : int | None = Field(default=None, foreign_key="genomesource.id")



class Genome(GenomeBase, table=True):

    id: int | None = Field(default=None, primary_key=True)
    # version : str | None = None
    # collection_releases : list[CollectionRelease] = Relationship(back_populates="genomes", link_model=GenomeCollectionReleaseLink)

    taxa : list[Taxon] =  Relationship(back_populates="genomes", link_model=GenomeTaxonLink)

    pangenome_links: list[GenomePangenomeLink] = Relationship(back_populates="genome")
    
    genome_source: GenomeSource = Relationship(back_populates="genomes")

class GenomePublic(GenomeBase):

    id:int
    genome_source : GenomeSourcePublic
    taxonomies : list["Taxonomy"]


class PangenomeBase(SQLModel):
    file_name : str
    annotation_source : str | None = None 

    collection_release_id: int | None = Field(default=None, foreign_key="collectionrelease.id")
    

class Pangenome(PangenomeBase, table=True):

    id: int | None = Field(default=None, primary_key=True)
    
    collection_release: CollectionRelease = Relationship(back_populates="pangenomes")

    genome_links: list[GenomePangenomeLink] = Relationship(back_populates="pangenome")


class PangenomePublic(PangenomeBase):
    id : int
    collection_release : CollectionReleasePublic
    taxonomy : "Taxonomy"


class TaxonomyBase(BaseModel):
    pass

class Taxonomy(TaxonomyBase):
    taxonomy_source: TaxonomySource
    taxa : list[Taxon]

class TaxonomyPublic(TaxonomyBase):
    taxonomy_source: TaxonomySourcePublic
    taxa : list[TaxonPublic]