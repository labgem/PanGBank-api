from datetime import datetime

from sqlalchemy import UniqueConstraint
from sqlmodel import Field, Relationship, SQLModel  # type: ignore


class GenomeTaxonLink(SQLModel, table=True):
    genome_id: int | None = Field(
        default=None, foreign_key="genome.id", primary_key=True
    )
    taxon_id: int | None = Field(default=None, foreign_key="taxon.id", primary_key=True)


class PangenomeTaxonLink(SQLModel, table=True):
    pangenome_id: int | None = Field(
        default=None, foreign_key="pangenome.id", primary_key=True
    )
    taxon_id: int | None = Field(default=None, foreign_key="taxon.id", primary_key=True)


class GenomeInPangenomeMetric(SQLModel):
    genome_name: str = Field(..., alias="Genome_name")
    contigs: int = Field(..., alias="Contigs")
    genes: int = Field(..., alias="Genes")
    fragmented_genes: int = Field(..., alias="Fragmented_genes")
    families: int = Field(..., alias="Families")
    families_with_fragments: int = Field(..., alias="Families_with_fragments")
    families_in_multicopy: int = Field(..., alias="Families_in_multicopy")
    soft_core_families: int = Field(..., alias="Soft_core_families")
    soft_core_genes: int = Field(..., alias="Soft_core_genes")
    exact_core_families: int = Field(..., alias="Exact_core_families")
    exact_core_genes: int = Field(..., alias="Exact_core_genes")
    persistent_genes: int = Field(..., alias="Persistent_genes")
    persistent_fragmented_genes: int = Field(..., alias="Persistent_fragmented_genes")
    persistent_families: int = Field(..., alias="Persistent_families")
    persistent_families_with_fragments: int = Field(
        ..., alias="Persistent_families_with_fragments"
    )
    persistent_families_in_multicopy: int = Field(
        ..., alias="Persistent_families_in_multicopy"
    )
    shell_genes: int = Field(..., alias="Shell_genes")
    shell_fragmented_genes: int = Field(..., alias="Shell_fragmented_genes")
    shell_families: int = Field(..., alias="Shell_families")
    shell_families_with_fragments: int = Field(
        ..., alias="Shell_families_with_fragments"
    )
    shell_families_in_multicopy: int = Field(..., alias="Shell_families_in_multicopy")
    cloud_genes: int = Field(..., alias="Cloud_genes")
    cloud_fragmented_genes: int = Field(..., alias="Cloud_fragmented_genes")
    cloud_families: int = Field(..., alias="Cloud_families")
    cloud_families_with_fragments: int = Field(
        ..., alias="Cloud_families_with_fragments"
    )
    cloud_families_in_multicopy: int = Field(..., alias="Cloud_families_in_multicopy")
    completeness: float = Field(..., alias="Completeness")
    contamination: float = Field(..., alias="Contamination")
    fragmentation: float = Field(..., alias="Fragmentation")
    rgps: int = Field(..., alias="RGPs")
    spots: int = Field(..., alias="Spots")
    modules: int = Field(..., alias="Modules")


class GenomePangenomeLink(GenomeInPangenomeMetric, table=True):
    __table_args__ = (UniqueConstraint("genome_id", "pangenome_id"),)

    id: int | None = Field(default=None, primary_key=True)
    genome_id: int | None = Field(default=None, foreign_key="genome.id")
    pangenome_id: int | None = Field(default=None, foreign_key="pangenome.id")

    pangenome: "Pangenome" = Relationship(back_populates="genome_links")
    genome: "Genome" = Relationship(back_populates="pangenome_links")

    genome_metadata: list["GenomeInPangenomeMetadata"] = Relationship(
        back_populates="genome_in_pangenome", cascade_delete=True
    )

    genome_file_md5sum: str
    genome_file_name: str


class CollectionBase(SQLModel):
    name: str = Field(unique=True)
    description: str | None = None


class Collection(CollectionBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

    releases: list["CollectionRelease"] = Relationship(
        back_populates="collection", cascade_delete=True
    )


class CollectionPublic(CollectionBase):
    id: int


class CollectionPublicWithReleases(CollectionPublic):
    releases: list["CollectionReleasePublicWithCount"]


class TaxonomySourceBase(SQLModel):
    name: str
    ranks: str
    version: str | None = None
    description: str | None = None
    source: str | None = None
    url: str | None = None


class TaxonomySource(TaxonomySourceBase, table=True):
    __table_args__ = (UniqueConstraint("name", "version", name="uq_name_version"),)

    id: int | None = Field(default=None, primary_key=True)
    name: str = Field()

    collection_releases: list["CollectionRelease"] = Relationship(
        back_populates="taxonomy_source"
    )
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

    release_note: str

    mash_version: str
    latest: bool = False
    date: datetime
    collection_id: int | None = Field(
        default=None, foreign_key="collection.id", ondelete="CASCADE"
    )

    # RESTRICT: Prevent the deletion of this record (CollectionReleaseBase)
    # if there is a foreign key value by raising an error.
    taxonomy_source_id: int | None = Field(
        default=None, foreign_key="taxonomysource.id", ondelete="RESTRICT"
    )


class CollectionRelease(CollectionReleaseBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

    mash_sketch: str
    mash_sketch_md5sum: str

    pangenomes_directory: str
    collection: "Collection" = Relationship(back_populates="releases")

    taxonomy_source: "TaxonomySource" = Relationship(
        back_populates="collection_releases"
    )

    pangenomes: list["Pangenome"] = Relationship(
        back_populates="collection_release", cascade_delete=True
    )


class CollectionReleasePublic(CollectionReleaseBase):
    id: int
    taxonomy_source: TaxonomySourcePublic
    collection_name: str
    collection: CollectionPublic

class CollectionReleasePublicWithCount(CollectionReleasePublic):
    pangenome_count: int


class TaxonBase(SQLModel):
    name: str = Field(index=True)
    rank: str
    depth: int

    taxid: int | None = None

    taxonomy_source_id: int | None = Field(
        index=True, default=None, foreign_key="taxonomysource.id"
    )


class Taxon(TaxonBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

    taxonomy_source: TaxonomySource = Relationship(back_populates="taxa")

    genomes: list["Genome"] = Relationship(
        back_populates="taxa", link_model=GenomeTaxonLink
    )
    pangenomes: list["Pangenome"] = Relationship(
        back_populates="taxa", link_model=PangenomeTaxonLink
    )


class TaxonPublic(TaxonBase):
    id: int


class GenomeBase(SQLModel):
    name: str = Field(unique=True, index=True)
    genome_source_id: int | None = Field(default=None, foreign_key="genomesource.id")


class Genome(GenomeBase, table=True):
    id: int | None = Field(default=None, primary_key=True)
    # version : str | None = None
    # collection_releases : list[CollectionRelease] = Relationship(back_populates="genomes",
    # link_model=GenomeCollectionReleaseLink)

    taxa: list[Taxon] = Relationship(
        back_populates="genomes", link_model=GenomeTaxonLink
    )

    pangenome_links: list[GenomePangenomeLink] = Relationship(back_populates="genome")

    genome_source: GenomeSource = Relationship(back_populates="genomes")

    genome_metadata: list["GenomeMetadata"] = Relationship(
        back_populates="genome", cascade_delete=True
    )


class GenomePublic(GenomeBase):
    id: int
    genome_source: GenomeSourcePublic


class GenomePublicWithTaxonomies(GenomePublic):
    taxonomies: list["TaxonomyPublic"]


class PangenomeMetric(SQLModel):
    # Metrics
    gene_count: int
    genome_count: int
    family_count: int
    edge_count: int

    # Persistent information
    persistent_family_count: int
    persistent_family_min_genome_frequency: float
    persistent_family_max_genome_frequency: float
    persistent_family_std_genome_frequency: float
    persistent_family_mean_genome_frequency: float

    # Shell information
    shell_family_count: int
    shell_family_min_genome_frequency: float
    shell_family_max_genome_frequency: float
    shell_family_std_genome_frequency: float
    shell_family_mean_genome_frequency: float

    # Cloud information
    cloud_family_count: int
    cloud_family_min_genome_frequency: float
    cloud_family_max_genome_frequency: float
    cloud_family_std_genome_frequency: float
    cloud_family_mean_genome_frequency: float

    partition_count: int
    rgp_count: int
    spot_count: int

    # Modules information
    module_count: int
    family_in_module_count: int


class PangenomeBase(PangenomeMetric):

    annotation_source: str | None = None
    name: str
    file_md5sum: str


class Pangenome(PangenomeBase, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    file_name: str

    collection_release_id: int | None = Field(
        default=None, foreign_key="collectionrelease.id"
    )

    collection_release: CollectionRelease = Relationship(back_populates="pangenomes")

    genome_links: list[GenomePangenomeLink] = Relationship(
        back_populates="pangenome", cascade_delete=True
    )

    taxa: list[Taxon] = Relationship(
        back_populates="pangenomes", link_model=PangenomeTaxonLink
    )


class PangenomePublic(PangenomeBase):
    id: int

    collection_release: CollectionReleasePublic

    taxonomy: "TaxonomyPublic"

    genome_count: int

    collection_release_id: int


class TaxonomyBase(SQLModel):
    pass


class Taxonomy(TaxonomyBase):
    taxonomy_source: TaxonomySource
    taxa: list[Taxon]


class TaxonomyPublic(TaxonomyBase):
    taxonomy_source: TaxonomySourcePublic
    taxa: list[TaxonPublic]


class MetadataBase(SQLModel):

    key: str
    value: str


class GenomeMetadata(MetadataBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

    source_id: int | None = Field(
        foreign_key="genomemetadatasource.id", default=None, ondelete="CASCADE"
    )

    genome_id: int | None = Field(
        foreign_key="genome.id", default=None, ondelete="CASCADE"
    )
    genome: Genome = Relationship(back_populates="genome_metadata")

    source: "GenomeMetadataSource" = Relationship(back_populates="genome_metadata")


class GenomeInPangenomeMetadata(MetadataBase, table=True):

    id: int | None = Field(default=None, primary_key=True)

    source_id: int | None = Field(
        foreign_key="genomeinpangenomemetadatasource.id",
        default=None,
        ondelete="CASCADE",
    )

    genome_pangenome_link_id: int | None = Field(
        foreign_key="genomepangenomelink.id",
        ondelete="CASCADE",
        default=None,
    )

    genome_in_pangenome: GenomePangenomeLink = Relationship(
        back_populates="genome_metadata",
    )

    source: "GenomeInPangenomeMetadataSource" = Relationship(
        back_populates="genome_metadata"
    )


class MetadataSourceBase(SQLModel):
    __table_args__ = (UniqueConstraint("name", "version", name="uq_name_version"),)

    id: int | None = Field(default=None, primary_key=True)
    name: str = Field()
    version: str | None = None
    description: str | None = None
    url: str | None = None


class GenomeMetadataSource(MetadataSourceBase, table=True):
    genome_metadata: list[GenomeMetadata] = Relationship(
        back_populates="source", cascade_delete=True
    )


class GenomeInPangenomeMetadataSource(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(unique=True)
    genome_metadata: list[GenomeInPangenomeMetadata] = Relationship(
        back_populates="source", cascade_delete=True
    )


class GenomePangenomeLinkPublic(GenomeInPangenomeMetric):
    genome_id: int
    pangenome_id: int

    genome_file_md5sum: str
    genome_file_name: str


class GenomePangenomeLinkWithMetadataPublic(GenomePangenomeLinkPublic):
    genome_metadata: list[MetadataBase]
