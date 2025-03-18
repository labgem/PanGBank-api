from pathlib import Path
from pangbank_api.models import (
    TaxonomySourceBase,
    GenomeSourceBase,
    Collection,
    CollectionRelease,
)
from pydantic import BaseModel, Field


class GenomeSourceInput(GenomeSourceBase):
    file: Path


class TaxonomySourceInput(TaxonomySourceBase):
    file: Path


class CollectionReleaseInput(BaseModel):
    collection: Collection
    release: CollectionRelease
    taxonomy: TaxonomySourceInput
    genome_sources: list[GenomeSourceInput] = Field(default_factory=list)
