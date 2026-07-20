"""Synchronous client for the PanGBank API."""

from __future__ import annotations

import httpx

from .transport import SyncTransport
from .resources.collections import CollectionsResource
from .resources.genomes import GenomesResource
from .resources.pangenomes import PangenomesResource


class PanGBankClient:
    """Synchronous entry point for the PanGBank API.

    Exposes the ``collections``, ``genomes``, and ``pangenomes`` resources,
    each backed by a shared :class:`~pangbank_api.sdk.transport.SyncTransport`.
    Can be used as a context manager to ensure the underlying HTTP client is
    closed.

    Example:
        >>> with PanGBankClient(base_url="https://api.pangbank.example") as client:
        ...     collections = client.collections.list()
    """

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 10.0,
        client: httpx.Client | None = None,
    ) -> None:
        """Create a new client.

        Args:
            base_url: Base URL of the PanGBank API. Required unless `client`
                is provided.
            timeout: Request timeout in seconds, used when a new
                `httpx.Client` is created from `base_url`.
            client: A pre-configured `httpx.Client` to use instead of
                creating one from `base_url`. When provided, its lifecycle
                is not managed by this client (`close` becomes a no-op).

        Raises:
            ValueError: If neither `base_url` nor `client` is provided.
        """
        self._transport = SyncTransport(
            base_url=base_url, timeout=timeout, client=client
        )
        self.collections = CollectionsResource(self._transport)
        self.genomes = GenomesResource(self._transport)
        self.pangenomes = PangenomesResource(self._transport)

    def close(self) -> None:
        """Close the underlying HTTP client, if owned by this instance."""
        self._transport.close()

    def __enter__(self) -> "PanGBankClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
