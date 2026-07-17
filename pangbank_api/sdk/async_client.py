from __future__ import annotations

import httpx

from .transport import AsyncTransport
from .resources.collections import AsyncCollectionsResource
from .resources.genomes import AsyncGenomesResource
from .resources.pangenomes import AsyncPangenomesResource


class AsyncPanGBankClient:
    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._transport = AsyncTransport(
            base_url=base_url, timeout=timeout, client=client
        )
        self.collections = AsyncCollectionsResource(self._transport)
        self.genomes = AsyncGenomesResource(self._transport)
        self.pangenomes = AsyncPangenomesResource(self._transport)

    async def close(self) -> None:
        await self._transport.close()

    async def __aenter__(self) -> "AsyncPanGBankClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()
