from sqlmodel import Field, Relationship, SQLModel

from datetime import datetime
from typing import Optional

class GenomeCollectionReleaseLink(SQLModel, table=True):
    genome_id: int | None = Field(default=None, foreign_key="genome.id", primary_key=True)
    collection_release_id: int | None = Field(default=None, foreign_key="collectionrelease.id", primary_key=True)


class GenomeTaxonLink(SQLModel, table=True):
    genome_id: int | None = Field(default=None, foreign_key="genome.id", primary_key=True)
    taxon_id: int | None = Field(default=None, foreign_key="taxon.id", primary_key=True)

    

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


class CollectionRelease(SQLModel, table=True):

    id: int | None = Field(default=None, primary_key=True)
    version : str

    ppanggolin_version : str
    pangbank_wf_version : str
    date: datetime = Field(default_factory=datetime.now, nullable=False)

    description : str | None = None
    state : str | None = None
    
    collection_id : int | None = Field(default=None, foreign_key="collection.id")
    collection: Collection = Relationship(back_populates="collection_releases")

    taxonomy_source_id : int | None = Field(default=None, foreign_key="taxonomysource.id")
    taxonomy_source : TaxonomySource = Relationship(back_populates="collection_releases")

    pangenomes: list["Pangenome"] = Relationship(back_populates="collection_release")


    genomes: list["Genome"] = Relationship(back_populates="collection_releases", link_model=GenomeCollectionReleaseLink)



# class Taxon(SQLModel, table=True):

#     id: int | None = Field(default=None, primary_key=True)
#     rank: str
#     name : str

#     parent_taxon_id: int | None = Field(default=None, foreign_key="taxon.id")
#     parent_taxon: Optional["Taxon"] = Relationship(back_populates="sub_taxa", sa_relationship_kwargs={"remote_side": "Taxon.id"})
#     sub_taxa: list["Taxon"] = Relationship(back_populates="parent_taxon")

#     taxonomy_release_id : int | None = Field(default=None, foreign_key="taxonomyrelease.id")
#     taxonomy_release : TaxonomySource = Relationship(back_populates="taxa")

#     genomes: list["Genome"] = Relationship(back_populates="taxa", link_model=GenomeTaxonLink)


# class Taxon(SQLModel, table=True):
#     id: int | None = Field(default=None, primary_key=True)
#     name: str
#     rank : str

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
#     version : str | None = None
    

    collection_releases : list[CollectionRelease] = Relationship(back_populates="genomes", link_model=GenomeCollectionReleaseLink)

    taxa : list[Taxon] =  Relationship(back_populates="genomes", link_model=GenomeTaxonLink)


class Pangenome(SQLModel, table=True):

    id: int | None = Field(default=None, primary_key=True)

    collection_release_id: int | None = Field(default=None, foreign_key="collectionrelease.id")
    collection_release: CollectionRelease = Relationship(back_populates="pangenomes")
    
    file_name: str
    
    # genes: int
    # genomes: int
    # families: int
    # edges: int
    
    # # Persistent
    # persistent_family_count: int
    
    # # Shell
    # shell_family_count: int
    
    # # Cloud
    # cloud_family_count: int
    
    # number_of_partitions: int
    
    # # Genomes fluidity
    # genomes_fluidity_all: float
    # genomes_fluidity_shell: float
    # genomes_fluidity_cloud: float
    # genomes_fluidity_accessory: float
    
    # rgp: int | None = None
    # spots: int | None = None
    
    # # Modules
    # number_of_modules: int | None = None
    # families_in_modules: int | None = None
    # partition_composition_persistent: float | None = None
    # partition_composition_shell: float | None = None
    # partition_composition_cloud: float | None = None
