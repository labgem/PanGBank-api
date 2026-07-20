"""Asynchronous client for the PanGBank API."""

from __future__ import annotations

import httpx

from .transport import AsyncTransport
from .resources.collections import AsyncCollectionsResource
from .resources.genomes import AsyncGenomesResource
from .resources.pangenomes import AsyncPangenomesResource


class AsyncPanGBankClient:
    """Asynchronous entry point for the PanGBank API.

    Exposes the ``collections``, ``genomes``, and ``pangenomes`` resources,
    each backed by a shared :class:`~pangbank_api.sdk.transport.AsyncTransport`.
    Can be used as an async context manager to ensure the underlying HTTP
    client is closed.

    Example:
        >>> async with AsyncPanGBankClient(base_url="https://api.pangbank.example") as client:
        ...     collections = await client.collections.list()
    """

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        """Create a new async client.

        Args:
            base_url: Base URL of the PanGBank API. Required unless `client`
                is provided.
            timeout: Request timeout in seconds, used when a new
                `httpx.AsyncClient` is created from `base_url`.
            client: A pre-configured `httpx.AsyncClient` to use instead of
                creating one from `base_url`. When provided, its lifecycle
                is not managed by this client (`close` becomes a no-op).

        Raises:
            ValueError: If neither `base_url` nor `client` is provided.
        """
        self._transport = AsyncTransport(
            base_url=base_url, timeout=timeout, client=client
        )
        self.collections = AsyncCollectionsResource(self._transport)
        self.genomes = AsyncGenomesResource(self._transport)
        self.pangenomes = AsyncPangenomesResource(self._transport)

    async def close(self) -> None:
        """Close the underlying HTTP client, if owned by this instance."""
        await self._transport.close()

    async def __aenter__(self) -> "AsyncPanGBankClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()
