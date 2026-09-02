"""Pangenomes resource: list, fetch, and download pangenome data and graphs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pangbank_api.models import GenomePangenomeLinkPublic, PangenomePublic

from ..transport import AsyncTransport, SyncTransport, ProgressCallback

def _filter_params(
    collection_name: str | None,
    collection_id: int | None,
    only_latest_release: bool | None,
    release_version: str | None,
    taxon_name: str | None,
    substring_taxon_match: bool,
    genome_name: str | None,
    pangenome_name: str | None,
) -> dict[str, Any]:
    return {
        "collection_name": collection_name,
        "collection_id": collection_id,
        "only_latest_release": only_latest_release,
        "release_version": release_version,
        "taxon_name": taxon_name,
        "substring_taxon_match": substring_taxon_match,
        "genome_name": genome_name,
        "pangenome_name": pangenome_name,
    }


class PangenomesResource:
    """Synchronous access to the `/pangenomes` endpoints."""

    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def list(
        self,
        collection_name: str | None = None,
        collection_id: int | None = None,
        only_latest_release: bool | None = None,
        release_version: str | None = None,
        taxon_name: str | None = None,
        substring_taxon_match: bool = False,
        genome_name: str | None = None,
        pangenome_name: str | None = None,
        offset: int = 0,
        limit: int = 20,
    ) -> list[PangenomePublic]:
        """List pangenomes, optionally filtered.

        Args:
            collection_name: Filter by the name of the owning collection.
            collection_id: Filter by the id of the owning collection.
            only_latest_release: If `True`, restrict to pangenomes from the
                latest release of their collection.
            release_version: Restrict to pangenomes from this collection
                release version.
            taxon_name: Filter by taxon name.
            substring_taxon_match: If `True`, match `taxon_name` as a
                substring instead of requiring an exact match.
            genome_name: Filter to pangenomes containing a genome with this
                name.
            pangenome_name: Filter by exact pangenome name.
            offset: Number of results to skip, for pagination.
            limit: Maximum number of results to return.

        Returns:
            Matching pangenomes.
        """

        params = _filter_params(
            collection_name,
            collection_id,
            only_latest_release,
            release_version,
            taxon_name,
            substring_taxon_match,
            genome_name,
            pangenome_name,
        )
        params.update({"offset": offset, "limit": limit})
        response = self._transport.get("/pangenomes/", params=params)
        return [PangenomePublic.model_validate(item) for item in response.json()]

    def get(self, pangenome_id: int) -> PangenomePublic:
        """Fetch a single pangenome by id.

        Args:
            pangenome_id: Id of the pangenome to fetch.

        Returns:
            The requested pangenome.

        Raises:
            PanGBankNotFoundError: If no pangenome with that id exists.
        """
        response = self._transport.get(f"/pangenomes/{pangenome_id}")
        return PangenomePublic.model_validate(response.json())

    def count(
        self,
        collection_name: str | None = None,
        collection_id: int | None = None,
        only_latest_release: bool | None = None,
        release_version: str | None = None,
        taxon_name: str | None = None,
        substring_taxon_match: bool = False,
        genome_name: str | None = None,
        pangenome_name: str | None = None,
    ) -> int:
        """Count pangenomes matching the given filters.

        Args:
            collection_name: Filter by the name of the owning collection.
            collection_id: Filter by the id of the owning collection.
            only_latest_release: If `True`, restrict to pangenomes from the
                latest release of their collection.
            release_version: Restrict to pangenomes from this collection
                release version.
            taxon_name: Filter by taxon name.
            substring_taxon_match: If `True`, match `taxon_name` as a
                substring instead of requiring an exact match.
            genome_name: Filter to pangenomes containing a genome with this
                name.
            pangenome_name: Filter by exact pangenome name.

        Returns:
            The number of matching pangenomes.
        """

        params = _filter_params(
            collection_name,
            collection_id,
            only_latest_release,
            release_version,
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
        """List the genomes belonging to a pangenome.

        Args:
            pangenome_id: Id of the pangenome.
            genome_name: Filter by exact genome name.
            offset: Number of results to skip, for pagination.
            limit: Maximum number of results to return.

        Returns:
            The genome-pangenome links for matching genomes.
        """
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
        """Fetch the link between a pangenome and one of its genomes.

        Args:
            pangenome_id: Id of the pangenome.
            genome_id: Id of the genome.

        Returns:
            The genome-pangenome link.

        Raises:
            PanGBankNotFoundError: If no such link exists.
        """
        response = self._transport.get(f"/pangenomes/{pangenome_id}/{genome_id}")
        return GenomePangenomeLinkPublic.model_validate(response.json())

    def download_file(
        self,
        pangenome_id: int,
        dest: str | Path | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> bytes | Path:
        """Download the pangenome file.

        Args:
            pangenome_id: Id of the pangenome.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.
            progress_callback: Optional callback called during download with
                `(downloaded_bytes, total_bytes)`. `total_bytes` is extracted
                from the Content-Length header when available.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """

        return self._transport.download(
            f"/pangenomes/{pangenome_id}/file",
            dest,
            progress_callback=progress_callback,
        )

    def download_cgview_map(
        self, pangenome_id: int, genome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the CGView map for a genome within a pangenome.

        Args:
            pangenome_id: Id of the pangenome.
            genome_id: Id of the genome.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return self._transport.download(
            f"/pangenomes/{pangenome_id}/{genome_id}/cgview_map", dest
        )

    def download_dbg_graph(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the De Bruijn graph file for a pangenome.

        Args:
            pangenome_id: Id of the pangenome.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return self._transport.download(f"/pangenomes/{pangenome_id}/dbg/graph", dest)

    def download_graph_tool(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the graph-tool representation of a pangenome.

        Args:
            pangenome_id: Id of the pangenome.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return self._transport.download(
            f"/pangenomes/{pangenome_id}/graph_tool", dest
        )

    def download_dbg_family_annotations(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the De Bruijn graph's gene family annotations.

        Args:
            pangenome_id: Id of the pangenome.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return self._transport.download(
            f"/pangenomes/{pangenome_id}/dbg/family_annotations", dest
        )

    def download_dbg_genome_annotations(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the De Bruijn graph's genome annotations.

        Args:
            pangenome_id: Id of the pangenome.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return self._transport.download(
            f"/pangenomes/{pangenome_id}/dbg/genome_annotations", dest
        )


class AsyncPangenomesResource:
    """Asynchronous access to the `/pangenomes` endpoints."""

    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def list(
        self,
        collection_name: str | None = None,
        collection_id: int | None = None,
        only_latest_release: bool | None = None,
        release_version: str | None = None,
        taxon_name: str | None = None,
        substring_taxon_match: bool = False,
        genome_name: str | None = None,
        pangenome_name: str | None = None,
        offset: int = 0,
        limit: int = 20,
    ) -> list[PangenomePublic]:
        """List pangenomes, optionally filtered.

        Args:
            collection_name: Filter by the name of the owning collection.
            collection_id: Filter by the id of the owning collection.
            only_latest_release: If `True`, restrict to pangenomes from the
                latest release of their collection.
            release_version: Restrict to pangenomes from this collection
                release version.
            taxon_name: Filter by taxon name.
            substring_taxon_match: If `True`, match `taxon_name` as a
                substring instead of requiring an exact match.
            genome_name: Filter to pangenomes containing a genome with this
                name.
            pangenome_name: Filter by exact pangenome name.
            offset: Number of results to skip, for pagination.
            limit: Maximum number of results to return.

        Returns:
            Matching pangenomes.
        """

        params = _filter_params(
            collection_name,
            collection_id,
            only_latest_release,
            release_version,
            taxon_name,
            substring_taxon_match,
            genome_name,
            pangenome_name,
        )
        params.update({"offset": offset, "limit": limit})
        response = await self._transport.get("/pangenomes/", params=params)
        return [PangenomePublic.model_validate(item) for item in response.json()]

    async def get(self, pangenome_id: int) -> PangenomePublic:
        """Fetch a single pangenome by id.

        Args:
            pangenome_id: Id of the pangenome to fetch.

        Returns:
            The requested pangenome.

        Raises:
            PanGBankNotFoundError: If no pangenome with that id exists.
        """
        response = await self._transport.get(f"/pangenomes/{pangenome_id}")
        return PangenomePublic.model_validate(response.json())

    async def count(
        self,
        collection_name: str | None = None,
        collection_id: int | None = None,
        only_latest_release: bool | None = None,
        release_version: str | None = None,
        taxon_name: str | None = None,
        substring_taxon_match: bool = False,
        genome_name: str | None = None,
        pangenome_name: str | None = None,
    ) -> int:
        """Count pangenomes matching the given filters.

        Args:
            collection_name: Filter by the name of the owning collection.
            collection_id: Filter by the id of the owning collection.
            only_latest_release: If `True`, restrict to pangenomes from the
                latest release of their collection.
            release_version: Restrict to pangenomes from this collection
                release version.
            taxon_name: Filter by taxon name.
            substring_taxon_match: If `True`, match `taxon_name` as a
                substring instead of requiring an exact match.
            genome_name: Filter to pangenomes containing a genome with this
                name.
            pangenome_name: Filter by exact pangenome name.

        Returns:
            The number of matching pangenomes.
        """

        params = _filter_params(
            collection_name,
            collection_id,
            only_latest_release,
            release_version,
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
        """List the genomes belonging to a pangenome.

        Args:
            pangenome_id: Id of the pangenome.
            genome_name: Filter by exact genome name.
            offset: Number of results to skip, for pagination.
            limit: Maximum number of results to return.

        Returns:
            The genome-pangenome links for matching genomes.
        """
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
        """Fetch the link between a pangenome and one of its genomes.

        Args:
            pangenome_id: Id of the pangenome.
            genome_id: Id of the genome.

        Returns:
            The genome-pangenome link.

        Raises:
            PanGBankNotFoundError: If no such link exists.
        """
        response = await self._transport.get(f"/pangenomes/{pangenome_id}/{genome_id}")
        return GenomePangenomeLinkPublic.model_validate(response.json())

    async def download_file(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the pangenome file.

        Args:
            pangenome_id: Id of the pangenome.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return await self._transport.download(f"/pangenomes/{pangenome_id}/file", dest)

    async def download_cgview_map(
        self, pangenome_id: int, genome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the CGView map for a genome within a pangenome.

        Args:
            pangenome_id: Id of the pangenome.
            genome_id: Id of the genome.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return await self._transport.download(
            f"/pangenomes/{pangenome_id}/{genome_id}/cgview_map", dest
        )

    async def download_dbg_graph(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the De Bruijn graph file for a pangenome.

        Args:
            pangenome_id: Id of the pangenome.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return await self._transport.download(
            f"/pangenomes/{pangenome_id}/dbg/graph", dest
        )

    async def download_graph_tool(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the graph-tool representation of a pangenome.

        Args:
            pangenome_id: Id of the pangenome.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return await self._transport.download(
            f"/pangenomes/{pangenome_id}/graph_tool", dest
        )

    async def download_dbg_family_annotations(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the De Bruijn graph's gene family annotations.

        Args:
            pangenome_id: Id of the pangenome.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return await self._transport.download(
            f"/pangenomes/{pangenome_id}/dbg/family_annotations", dest
        )

    async def download_dbg_genome_annotations(
        self, pangenome_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the De Bruijn graph's genome annotations.

        Args:
            pangenome_id: Id of the pangenome.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return await self._transport.download(
            f"/pangenomes/{pangenome_id}/dbg/genome_annotations", dest
        )
