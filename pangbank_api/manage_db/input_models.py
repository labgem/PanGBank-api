from pathlib import Path
from pangbank_api.models import (
    TaxonomySourceBase,
    GenomeSourceBase,
    Collection,
    CollectionRelease,
    MetadataSourceBase,
    MetadataBase,
)
from pydantic import BaseModel, Field


class GenomeSourceInput(GenomeSourceBase):
    file: Path


class TaxonomySourceInput(TaxonomySourceBase):
    file: Path


class GenomeMetadataSourceInput(MetadataSourceBase):
    file: Path
    genome_name_to_genome_metadata: dict[str, list[MetadataBase]] = Field(
        default_factory=dict
    )


class CollectionReleaseInput(BaseModel):
    collection: Collection
    release: CollectionRelease
    taxonomy: TaxonomySourceInput
    genome_sources: list[GenomeSourceInput] = Field(default_factory=list)
    genome_metadata_sources: list[GenomeMetadataSourceInput] = Field(
        default_factory=list
    )
