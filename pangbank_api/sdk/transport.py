"""HTTP transport layer shared by the sync and async PanGBank clients.

Wraps `httpx` clients to translate error responses and connection failures
into the SDK's exception types, and to centralize query-parameter cleanup
and streaming downloads.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx

from .exceptions import PanGBankAPIError, PanGBankConnectionError, PanGBankNotFoundError


DEFAULT_BASE_URL = "https://pangbank-api.genoscope.cns.fr/"


def _response_detail(response: httpx.Response) -> str:
    """Extract a human-readable error message from an API response.

    Prefers the JSON `detail` field (as returned by FastAPI's error
    responses); falls back to the raw response body.
    """
    try:
        body = response.json()
    except ValueError:
        return response.text
    if isinstance(body, dict) and "detail" in body:
        return str(body["detail"])  # type: ignore
    return response.text


def _raise_for_response(response: httpx.Response) -> None:
    """Raise a PanGBank exception if `response` represents an error.

    Raises:
        PanGBankNotFoundError: If the response status is 404.
        PanGBankAPIError: For any other error status code.
    """
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
    """Drop `None`-valued entries so optional filters are omitted from the query string."""
    if params is None:
        return {}
    return {key: value for key, value in params.items() if value is not None}


class SyncTransport:
    """Synchronous HTTP transport backed by `httpx.Client`.

    Handles request dispatch, error translation, and file downloads for
    :class:`~pangbank_api.sdk.client.PanGBankClient` and its resources.
    """

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 10.0,
        client: httpx.Client | None = None,
    ) -> None:
        """Create a new sync transport.

        Args:
            base_url: Base URL of the PanGBank API. Required unless `client`
                is provided.
            timeout: Request timeout in seconds, used when a new
                `httpx.Client` is created from `base_url`.
            client: A pre-configured `httpx.Client` to use instead of
                creating one from `base_url`. When provided, its lifecycle
                is not managed by this transport (`close` becomes a no-op).

        Raises:
            ValueError: If neither `base_url` nor `client` is provided.
        """
        if client is not None:
            self._client = client
            self._owns_client = False
        elif base_url is not None:
            self._client = httpx.Client(base_url=base_url, timeout=timeout)
            self._owns_client = True
        else:
            raise ValueError("Either base_url or client must be provided")

    def get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        """Perform a GET request and raise on error responses.

        Args:
            path: Request path, relative to the client's base URL.
            params: Query parameters; entries with a `None` value are omitted.

        Returns:
            The successful `httpx.Response`.

        Raises:
            PanGBankConnectionError: If the request could not be completed.
            PanGBankNotFoundError: If the API responds with 404.
            PanGBankAPIError: If the API responds with another error status.
        """
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
        """Stream a GET response to memory or to a file.

        Args:
            path: Request path, relative to the client's base URL.
            dest: If given, the response body is streamed to this file path
                and the path is returned. If `None`, the full body is
                buffered in memory and returned as `bytes`.
            params: Query parameters; entries with a `None` value are omitted.

        Returns:
            The downloaded content as `bytes`, or the `Path` written to if
            `dest` was given.

        Raises:
            PanGBankConnectionError: If the request could not be completed.
            PanGBankNotFoundError: If the API responds with 404.
            PanGBankAPIError: If the API responds with another error status.
        """
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
        """Close the underlying HTTP client, if owned by this instance."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "SyncTransport":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class AsyncTransport:
    """Asynchronous HTTP transport backed by `httpx.AsyncClient`.

    Handles request dispatch, error translation, and file downloads for
    :class:`~pangbank_api.sdk.async_client.AsyncPanGBankClient` and its
    resources.
    """

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        """Create a new async transport.

        Args:
            base_url: Base URL of the PanGBank API. Required unless `client`
                is provided.
            timeout: Request timeout in seconds, used when a new
                `httpx.AsyncClient` is created from `base_url`.
            client: A pre-configured `httpx.AsyncClient` to use instead of
                creating one from `base_url`. When provided, its lifecycle
                is not managed by this transport (`close` becomes a no-op).

        Raises:
            ValueError: If neither `base_url` nor `client` is provided.
        """
        if client is not None:
            self._client = client
            self._owns_client = False
        elif base_url is not None:
            self._client = httpx.AsyncClient(base_url=base_url, timeout=timeout)
            self._owns_client = True
        else:
            raise ValueError("Either base_url or client must be provided")

    async def get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        """Perform a GET request and raise on error responses.

        Args:
            path: Request path, relative to the client's base URL.
            params: Query parameters; entries with a `None` value are omitted.

        Returns:
            The successful `httpx.Response`.

        Raises:
            PanGBankConnectionError: If the request could not be completed.
            PanGBankNotFoundError: If the API responds with 404.
            PanGBankAPIError: If the API responds with another error status.
        """
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
        """Stream a GET response to memory or to a file.

        Args:
            path: Request path, relative to the client's base URL.
            dest: If given, the response body is streamed to this file path
                and the path is returned. If `None`, the full body is
                buffered in memory and returned as `bytes`.
            params: Query parameters; entries with a `None` value are omitted.

        Returns:
            The downloaded content as `bytes`, or the `Path` written to if
            `dest` was given.

        Raises:
            PanGBankConnectionError: If the request could not be completed.
            PanGBankNotFoundError: If the API responds with 404.
            PanGBankAPIError: If the API responds with another error status.
        """
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
        """Close the underlying HTTP client, if owned by this instance."""
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "AsyncTransport":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()
