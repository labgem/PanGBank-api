from __future__ import annotations

import httpx

from .transport import SyncTransport
from .resources.collections import CollectionsResource
from .resources.genomes import GenomesResource
from .resources.pangenomes import PangenomesResource


class PanGBankClient:
    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 10.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._transport = SyncTransport(
            base_url=base_url, timeout=timeout, client=client
        )
        self.collections = CollectionsResource(self._transport)
        self.genomes = GenomesResource(self._transport)
        self.pangenomes = PangenomesResource(self._transport)

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> "PanGBankClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
