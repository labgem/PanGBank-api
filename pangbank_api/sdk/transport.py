from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx

from .exceptions import PanGBankAPIError, PanGBankConnectionError, PanGBankNotFoundError


def _response_detail(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text
    if isinstance(body, dict) and "detail" in body:
        return str(body["detail"])
    return response.text


def _raise_for_response(response: httpx.Response) -> None:
    if response.status_code == 404:
        raise PanGBankNotFoundError(
            _response_detail(response),
            status_code=response.status_code,
            response=response,
        )
    if response.is_error:
        raise PanGBankAPIError(
            _response_detail(response),
            status_code=response.status_code,
            response=response,
        )


def _clean_params(params: dict[str, Any] | None) -> dict[str, Any]:
    if params is None:
        return {}
    return {key: value for key, value in params.items() if value is not None}


class SyncTransport:
    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 10.0,
        client: httpx.Client | None = None,
    ) -> None:
        if client is not None:
            self._client = client
            self._owns_client = False
        elif base_url is not None:
            self._client = httpx.Client(base_url=base_url, timeout=timeout)
            self._owns_client = True
        else:
            raise ValueError("Either base_url or client must be provided")

    def get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        try:
            response = self._client.get(path, params=_clean_params(params))
        except httpx.HTTPError as exc:
            raise PanGBankConnectionError(str(exc)) from exc
        _raise_for_response(response)
        return response

    def download(
        self,
        path: str,
        dest: str | Path | None = None,
        params: dict[str, Any] | None = None,
    ) -> bytes | Path:
        try:
            with self._client.stream("GET", path, params=_clean_params(params)) as response:
                if response.is_error:
                    response.read()
                    _raise_for_response(response)
                if dest is None:
                    response.read()
                    return response.content
                dest_path = Path(dest)
                with dest_path.open("wb") as file_obj:
                    for chunk in response.iter_bytes():
                        file_obj.write(chunk)
                return dest_path
        except httpx.HTTPError as exc:
            raise PanGBankConnectionError(str(exc)) from exc

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "SyncTransport":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class AsyncTransport:
    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if client is not None:
            self._client = client
            self._owns_client = False
        elif base_url is not None:
            self._client = httpx.AsyncClient(base_url=base_url, timeout=timeout)
            self._owns_client = True
        else:
            raise ValueError("Either base_url or client must be provided")

    async def get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        try:
            response = await self._client.get(path, params=_clean_params(params))
        except httpx.HTTPError as exc:
            raise PanGBankConnectionError(str(exc)) from exc
        _raise_for_response(response)
        return response

    async def download(
        self,
        path: str,
        dest: str | Path | None = None,
        params: dict[str, Any] | None = None,
    ) -> bytes | Path:
        try:
            async with self._client.stream(
                "GET", path, params=_clean_params(params)
            ) as response:
                if response.is_error:
                    await response.aread()
                    _raise_for_response(response)
                if dest is None:
                    await response.aread()
                    return response.content
                dest_path = Path(dest)
                with dest_path.open("wb") as file_obj:
                    async for chunk in response.aiter_bytes():
                        file_obj.write(chunk)
                return dest_path
        except httpx.HTTPError as exc:
            raise PanGBankConnectionError(str(exc)) from exc

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "AsyncTransport":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()
