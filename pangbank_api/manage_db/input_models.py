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


class GenomeQualityMetricsInput(BaseModel):
    file: Path


class GenomeStatusInput(BaseModel):
    """Input for genome status (representative/reference) information."""

    status_type: str  # "representative", "reference", etc.
    origin: str  # "GTDB RS220", "NCBI RefSeq", etc.
    file: Path  # Path to text file with genome names (one per line)


class CollectionReleaseInput(BaseModel):
    collection: Collection
    release: CollectionRelease
    taxonomy: TaxonomySourceInput
    genome_sources: list[GenomeSourceInput] = Field(
        default_factory=list
    )  # pyright: ignore[reportUnknownVariableType]
    genome_quality_metrics: GenomeQualityMetricsInput | None = None
    genome_statuses: list[GenomeStatusInput] = Field(
        default_factory=list
    )  # pyright: ignore[reportUnknownVariableType]
