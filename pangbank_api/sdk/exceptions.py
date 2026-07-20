"""Exceptions raised by the PanGBank SDK."""

from __future__ import annotations

import httpx


class PanGBankAPIError(Exception):
    """Base exception for errors returned by the PanGBank API.

    Raised when the API responds with an error status code that isn't
    handled by a more specific subclass.

    Attributes:
        status_code: The HTTP status code of the response, if available.
        response: The underlying ``httpx.Response``, if available.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        response: httpx.Response | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class PanGBankNotFoundError(PanGBankAPIError):
    """Raised when the API returns a 404 for a requested resource."""


class PanGBankConnectionError(PanGBankAPIError):
    """Raised when the underlying HTTP request could not be completed."""

    def __init__(self, message: str) -> None:
        super().__init__(message, status_code=None, response=None)
