from __future__ import annotations

from pathlib import Path
from typing import Any

from pangbank_api.models import CollectionPublicWithReleases

from ..transport import AsyncTransport, SyncTransport


def _list_params(
    collection_name: str | None,
    collection_id: int | None,
    only_latest_release: bool | None,
) -> dict[str, Any]:
    return {
        "collection_name": collection_name,
        "collection_id": collection_id,
        "only_latest_release": only_latest_release,
    }


class CollectionsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def list(
        self,
        collection_name: str | None = None,
        collection_id: int | None = None,
        only_latest_release: bool | None = None,
    ) -> list[CollectionPublicWithReleases]:
        response = self._transport.get(
            "/collections/",
            params=_list_params(collection_name, collection_id, only_latest_release),
        )
        return [
            CollectionPublicWithReleases.model_validate(item)
            for item in response.json()
        ]

    def get(
        self, collection_id: int, only_latest_release: bool | None = None
    ) -> CollectionPublicWithReleases:
        response = self._transport.get(
            f"/collections/{collection_id}",
            params={"only_latest_release": only_latest_release},
        )
        return CollectionPublicWithReleases.model_validate(response.json())

    def download_mash_sketch(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return self._transport.download(
            f"/collections/{collection_id}/mash_sketch", dest
        )

    def download_index_info(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return self._transport.download(
            f"/collections/{collection_id}/index/info", dest
        )

    def download_index_pangenomes(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return self._transport.download(
            f"/collections/{collection_id}/index/pangenomes", dest
        )

    def download_index_genomes(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return self._transport.download(
            f"/collections/{collection_id}/index/genomes", dest
        )


class AsyncCollectionsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def list(
        self,
        collection_name: str | None = None,
        collection_id: int | None = None,
        only_latest_release: bool | None = None,
    ) -> list[CollectionPublicWithReleases]:
        response = await self._transport.get(
            "/collections/",
            params=_list_params(collection_name, collection_id, only_latest_release),
        )
        return [
            CollectionPublicWithReleases.model_validate(item)
            for item in response.json()
        ]

    async def get(
        self, collection_id: int, only_latest_release: bool | None = None
    ) -> CollectionPublicWithReleases:
        response = await self._transport.get(
            f"/collections/{collection_id}",
            params={"only_latest_release": only_latest_release},
        )
        return CollectionPublicWithReleases.model_validate(response.json())

    async def download_mash_sketch(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return await self._transport.download(
            f"/collections/{collection_id}/mash_sketch", dest
        )

    async def download_index_info(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return await self._transport.download(
            f"/collections/{collection_id}/index/info", dest
        )

    async def download_index_pangenomes(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return await self._transport.download(
            f"/collections/{collection_id}/index/pangenomes", dest
        )

    async def download_index_genomes(
        self, collection_id: int, dest: str | Path | None = None
    ) -> bytes | Path:
        return await self._transport.download(
            f"/collections/{collection_id}/index/genomes", dest
        )
