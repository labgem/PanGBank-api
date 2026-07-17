from __future__ import annotations

from typing import Any

from pangbank_api.models import GenomePublic

from ..transport import AsyncTransport, SyncTransport


def _list_params(
    genome_name: str | None,
    taxon_name: str | None,
    substring_taxon_match: bool,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    return {
        "genome_name": genome_name,
        "taxon_name": taxon_name,
        "substring_taxon_match": substring_taxon_match,
        "offset": offset,
        "limit": limit,
    }


class GenomesResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def list(
        self,
        genome_name: str | None = None,
        taxon_name: str | None = None,
        substring_taxon_match: bool = False,
        offset: int = 0,
        limit: int = 20,
    ) -> list[GenomePublic]:
        response = self._transport.get(
            "/genomes/",
            params=_list_params(
                genome_name, taxon_name, substring_taxon_match, offset, limit
            ),
        )
        return [GenomePublic.model_validate(item) for item in response.json()]

    def get(self, genome_id: int) -> GenomePublic:
        response = self._transport.get(f"/genomes/{genome_id}")
        return GenomePublic.model_validate(response.json())


class AsyncGenomesResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def list(
        self,
        genome_name: str | None = None,
        taxon_name: str | None = None,
        substring_taxon_match: bool = False,
        offset: int = 0,
        limit: int = 20,
    ) -> list[GenomePublic]:
        response = await self._transport.get(
            "/genomes/",
            params=_list_params(
                genome_name, taxon_name, substring_taxon_match, offset, limit
            ),
        )
        return [GenomePublic.model_validate(item) for item in response.json()]

    async def get(self, genome_id: int) -> GenomePublic:
        response = await self._transport.get(f"/genomes/{genome_id}")
        return GenomePublic.model_validate(response.json())
