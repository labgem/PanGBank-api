try:
    import httpx  # noqa: F401
except ImportError as exc:
    raise ImportError(
        "httpx is required to use the PanGBank SDK. "
        "Install it with: pip install pangbank-api[sdk]"
    ) from exc

from .async_client import AsyncPanGBankClient
from .client import PanGBankClient
from .exceptions import (
    PanGBankAPIError,
    PanGBankConnectionError,
    PanGBankNotFoundError,
)

__all__ = [
    "PanGBankClient",
    "AsyncPanGBankClient",
    "PanGBankAPIError",
    "PanGBankNotFoundError",
    "PanGBankConnectionError",
]
