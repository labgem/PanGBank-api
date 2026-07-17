from __future__ import annotations

from pathlib import Path
from typing import Any

from pangbank_api.models import GenomePangenomeLinkPublic, PangenomePublic

from ..transport import AsyncTransport, SyncTransport


def _filter_params(
    collection_name: str | None,
    collection_id: int | None,
    only_latest_release: bool | None,
    taxon_name: str | None,
    substring_taxon_match: bool,
    genome_name: str | None,
    pangenome_name: str | None,
) -> dict[str, Any]:
    return {
        "collection_name": collection_name,
        "collection_id": collection_id,
        "only_latest_release": only_latest_release,
        "taxon_name": taxon_name,
        "substring_taxon_match": substring_taxon_match,
        "genome_name": genome_name,
        "pangenome_name": pangenome_name,
    }


class PangenomesResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def list(
        self,
        collection_name: str | None = None,
        collection_id: int | None = None,
        only_latest_release: bool | None = None,
        taxon_name: str | None = None,
        substring_taxon_match: bool = False,
        genome_name: str | None = None,
        pangenome_name: str | None = None,
        offset: int = 0,
        limit: int = 20,
    ) -> list[PangenomePublic]:
        params = _filter_params(
            collection_name,
            collection_id,
            only_latest_release,
            taxon_name,
            substring_taxon_match,
            genome_name,
            pangenome_name,
        )
        params.update({"offset": offset, "limit": limit})
        response = self._transport.get("/pangenomes/", params=params)
        return [PangenomePublic.model_validate(item) for item in response.json()]

    def get(self, pangenome_id: int) -> PangenomePublic:
        response = self._transport.get(f"/pangenomes/{pangenome_id}")
        return PangenomePublic.model_validate(response.json())

    def count(
        self,
        collection_name: str | None = None,
        collection_id: int | None = None,
        only_latest_release: bool | None = None,
        taxon_name: str | None = None,
        substring_taxon_match: bool = False,
        genome_name: str | None = None,
        pangenome_name: str | None = None,
    ) -> int:
        params = _filter_params(
            collection_name,
            collection_id,
            only_latest_release,
            taxon_name,
            substring_taxon_match,
            genome_name,
            pangenome_name,
        )
        response = self._transport.get("/pangenomes/count/", params=params)
        return response.json()

    def list_genomes(
        self,
        pangenome_id: int,
        genome_name: str | None = None,
        offset: int = 0,
        limit: int = 20,
    ) -> list[GenomePangenomeLinkPublic]:
        response = self._transport.get(
            f"/pangenomes/{pangenome_id}/genomes",
            params={"genome_name": genome_name, "offset": offset, "limit": limit},
        )
        return [
            GenomePangenomeLinkPublic.model_validate(item) for item in response.json()
        ]

    def get_genome(
        self, pangenome_id: int, genome_id: int
    ) -> GenomePangenomeLinkPublic:
        response = self._transport.get(f"/pangenomes/{pangenome_id}/{genome_id}")
        return GenomePangenomeLinkPublic.model_validate(response.json())

    def download_file(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return self._transport.download(f"/pangenomes/{pangenome_id}/file", dest)

    def download_cgview_map(
        self, pangenome_id: int, genome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return self._transport.download(
            f"/pangenomes/{pangenome_id}/{genome_id}/cgview_map", dest
        )

    def download_dbg_graph(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return self._transport.download(f"/pangenomes/{pangenome_id}/dbg/graph", dest)

    def download_graph_tool(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return self._transport.download(
            f"/pangenomes/{pangenome_id}/graph_tool", dest
        )

    def download_dbg_family_annotations(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return self._transport.download(
            f"/pangenomes/{pangenome_id}/dbg/family_annotations", dest
        )

    def download_dbg_genome_annotations(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return self._transport.download(
            f"/pangenomes/{pangenome_id}/dbg/genome_annotations", dest
        )


class AsyncPangenomesResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def list(
        self,
        collection_name: str | None = None,
        collection_id: int | None = None,
        only_latest_release: bool | None = None,
        taxon_name: str | None = None,
        substring_taxon_match: bool = False,
        genome_name: str | None = None,
        pangenome_name: str | None = None,
        offset: int = 0,
        limit: int = 20,
    ) -> list[PangenomePublic]:
        params = _filter_params(
            collection_name,
            collection_id,
            only_latest_release,
            taxon_name,
            substring_taxon_match,
            genome_name,
            pangenome_name,
        )
        params.update({"offset": offset, "limit": limit})
        response = await self._transport.get("/pangenomes/", params=params)
        return [PangenomePublic.model_validate(item) for item in response.json()]

    async def get(self, pangenome_id: int) -> PangenomePublic:
        response = await self._transport.get(f"/pangenomes/{pangenome_id}")
        return PangenomePublic.model_validate(response.json())

    async def count(
        self,
        collection_name: str | None = None,
        collection_id: int | None = None,
        only_latest_release: bool | None = None,
        taxon_name: str | None = None,
        substring_taxon_match: bool = False,
        genome_name: str | None = None,
        pangenome_name: str | None = None,
    ) -> int:
        params = _filter_params(
            collection_name,
            collection_id,
            only_latest_release,
            taxon_name,
            substring_taxon_match,
            genome_name,
            pangenome_name,
        )
        response = await self._transport.get("/pangenomes/count/", params=params)
        return response.json()

    async def list_genomes(
        self,
        pangenome_id: int,
        genome_name: str | None = None,
        offset: int = 0,
        limit: int = 20,
    ) -> list[GenomePangenomeLinkPublic]:
        response = await self._transport.get(
            f"/pangenomes/{pangenome_id}/genomes",
            params={"genome_name": genome_name, "offset": offset, "limit": limit},
        )
        return [
            GenomePangenomeLinkPublic.model_validate(item) for item in response.json()
        ]

    async def get_genome(
        self, pangenome_id: int, genome_id: int
    ) -> GenomePangenomeLinkPublic:
        response = await self._transport.get(f"/pangenomes/{pangenome_id}/{genome_id}")
        return GenomePangenomeLinkPublic.model_validate(response.json())

    async def download_file(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return await self._transport.download(f"/pangenomes/{pangenome_id}/file", dest)

    async def download_cgview_map(
        self, pangenome_id: int, genome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return await self._transport.download(
            f"/pangenomes/{pangenome_id}/{genome_id}/cgview_map", dest
        )

    async def download_dbg_graph(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return await self._transport.download(
            f"/pangenomes/{pangenome_id}/dbg/graph", dest
        )

    async def download_graph_tool(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return await self._transport.download(
            f"/pangenomes/{pangenome_id}/graph_tool", dest
        )

    async def download_dbg_family_annotations(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return await self._transport.download(
            f"/pangenomes/{pangenome_id}/dbg/family_annotations", dest
        )

    async def download_dbg_genome_annotations(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return await self._transport.download(
            f"/pangenomes/{pangenome_id}/dbg/genome_annotations", dest
        )
