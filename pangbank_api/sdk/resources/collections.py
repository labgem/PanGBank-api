"""Collections resource: list, fetch, and download collection assets."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pangbank_api.models import CollectionPublicWithReleases

from ..transport import AsyncTransport, SyncTransport


def _list_params(
    collection_name: str | None,
    collection_id: int | None,
    only_latest_release: bool | None,
    release_version: str | None = None,
) -> dict[str, Any]:
    return {
        "collection_name": collection_name,
        "collection_id": collection_id,
        "only_latest_release": only_latest_release,
        "release_version": release_version,
    }


def _release_version_params(release_version: str | None) -> dict[str, Any]:
    return {"release_version": release_version}


class CollectionsResource:
    """Synchronous access to the `/collections` endpoints."""

    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def list(
        self,
        collection_name: str | None = None,
        collection_id: int | None = None,
        only_latest_release: bool | None = None,
        release_version: str | None = None,
    ) -> list[CollectionPublicWithReleases]:
        """List collections, optionally filtered.

        Args:
            collection_name: Filter by exact collection name.
            collection_id: Filter by collection id.
            only_latest_release: If `True`, restrict each collection's
                releases to the latest one.

        Returns:
            Matching collections, each with its release(s).
        """
        response = self._transport.get(
            "/collections/",
            params=_list_params(
                collection_name, collection_id, only_latest_release, release_version
            ),
        )
        return [
            CollectionPublicWithReleases.model_validate(item)
            for item in response.json()
        ]

    def get(
        self,
        collection_id: int,
        only_latest_release: bool | None = None,
        release_version: str | None = None,
    ) -> CollectionPublicWithReleases:
        """Fetch a single collection by id.

        Args:
            collection_id: Id of the collection to fetch.
            only_latest_release: If `True`, restrict the returned releases
                to the latest one.

        Returns:
            The requested collection with its release(s).

        Raises:
            PanGBankNotFoundError: If no collection with that id exists.
        """
        response = self._transport.get(
            f"/collections/{collection_id}",
            params={
                "only_latest_release": only_latest_release,
                "release_version": release_version,
            },
        )
        return CollectionPublicWithReleases.model_validate(response.json())

    def download_mash_sketch(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the Mash sketch file for a collection.

        Args:
            collection_id: Id of the collection.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return self._transport.download(
            f"/collections/{collection_id}/mash_sketch", dest
        )

    def download_index_info(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the index info file for a collection.

        Args:
            collection_id: Id of the collection.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return self._transport.download(
            f"/collections/{collection_id}/index/info", dest
        )

    def download_index_pangenomes(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the pangenomes index file for a collection.

        Args:
            collection_id: Id of the collection.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return self._transport.download(
            f"/collections/{collection_id}/index/pangenomes", dest
        )

    def download_index_genomes(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the genomes index file for a collection.

        Args:
            collection_id: Id of the collection.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return self._transport.download(
            f"/collections/{collection_id}/index/genomes", dest
        )

    def get_multiqc_report(
        self, collection_id: int, release_version: str | None = None
    ) -> str:
        """Get the MultiQC HTML report for a collection release.

        Args:
            collection_id: Id of the collection.
            release_version: Optional release version to target.

        Returns:
            The MultiQC report content as HTML text.
        """
        response = self._transport.get(
            f"/collections/{collection_id}/multiqc_report",
            params=_release_version_params(release_version),
        )
        return response.text

    def get_release_notes(
        self, collection_id: int, release_version: str | None = None
    ) -> str:
        """Get the release notes for a collection release.

        Args:
            collection_id: Id of the collection.
            release_version: Optional release version to target.

        Returns:
            The release notes as plain text.
        """
        response = self._transport.get(
            f"/collections/{collection_id}/release_notes",
            params=_release_version_params(release_version),
        )
        return response.text


class AsyncCollectionsResource:
    """Asynchronous access to the `/collections` endpoints."""

    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def list(
        self,
        collection_name: str | None = None,
        collection_id: int | None = None,
        only_latest_release: bool | None = None,
        release_version: str | None = None,
    ) -> list[CollectionPublicWithReleases]:
        """List collections, optionally filtered.

        Args:
            collection_name: Filter by exact collection name.
            collection_id: Filter by collection id.
            only_latest_release: If `True`, restrict each collection's
                releases to the latest one.

        Returns:
            Matching collections, each with its release(s).
        """
        response = await self._transport.get(
            "/collections/",
            params=_list_params(
                collection_name, collection_id, only_latest_release, release_version
            ),
        )
        return [
            CollectionPublicWithReleases.model_validate(item)
            for item in response.json()
        ]

    async def get(
        self,
        collection_id: int,
        only_latest_release: bool | None = None,
        release_version: str | None = None,
    ) -> CollectionPublicWithReleases:
        """Fetch a single collection by id.

        Args:
            collection_id: Id of the collection to fetch.
            only_latest_release: If `True`, restrict the returned releases
                to the latest one.

        Returns:
            The requested collection with its release(s).

        Raises:
            PanGBankNotFoundError: If no collection with that id exists.
        """
        response = await self._transport.get(
            f"/collections/{collection_id}",
            params={
                "only_latest_release": only_latest_release,
                "release_version": release_version,
            },
        )
        return CollectionPublicWithReleases.model_validate(response.json())

    async def download_mash_sketch(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the Mash sketch file for a collection.

        Args:
            collection_id: Id of the collection.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return await self._transport.download(
            f"/collections/{collection_id}/mash_sketch", dest
        )

    async def download_index_info(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the index info file for a collection.

        Args:
            collection_id: Id of the collection.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return await self._transport.download(
            f"/collections/{collection_id}/index/info", dest
        )

    async def download_index_pangenomes(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the pangenomes index file for a collection.

        Args:
            collection_id: Id of the collection.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return await self._transport.download(
            f"/collections/{collection_id}/index/pangenomes", dest
        )

    async def download_index_genomes(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        """Download the genomes index file for a collection.

        Args:
            collection_id: Id of the collection.
            dest: If given, write the file to this path and return it;
                otherwise return the raw content as `bytes`.

        Returns:
            The file content as `bytes`, or the `Path` written to.
        """
        return await self._transport.download(
            f"/collections/{collection_id}/index/genomes", dest
        )

    async def get_multiqc_report(
        self, collection_id: int, release_version: str | None = None
    ) -> str:
        """Get the MultiQC HTML report for a collection release.

        Args:
            collection_id: Id of the collection.
            release_version: Optional release version to target.

        Returns:
            The MultiQC report content as HTML text.
        """
        response = await self._transport.get(
            f"/collections/{collection_id}/multiqc_report",
            params=_release_version_params(release_version),
        )
        return response.text

    async def get_release_notes(
        self, collection_id: int, release_version: str | None = None
    ) -> str:
        """Get the release notes for a collection release.

        Args:
            collection_id: Id of the collection.
            release_version: Optional release version to target.

        Returns:
            The release notes as plain text.
        """
        response = await self._transport.get(
            f"/collections/{collection_id}/release_notes",
            params=_release_version_params(release_version),
        )
        return response.text
