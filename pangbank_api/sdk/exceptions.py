from __future__ import annotations

import httpx


class PanGBankAPIError(Exception):
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
