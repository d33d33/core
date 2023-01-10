"""Hub for jarvis integration."""
from __future__ import annotations

import logging

import httpx

from homeassistant.exceptions import HomeAssistantError

_LOGGER = logging.getLogger(__name__)


class JarvisHub:
    """Placeholder class to make tests pass."""

    def __init__(self, host: str) -> None:
        """Initialize."""
        self.host = host

    async def test_connection(self) -> bool:
        """Test if we can connect with the host."""
        async with httpx.AsyncClient() as client:
            try:
                res = await client.get(self.host)
                res.raise_for_status()
                return True
            except httpx.HTTPStatusError:
                return False
            except httpx.UnsupportedProtocol as exc:
                raise InvalidHost from exc
            except httpx.RequestError as exc:
                raise CannotConnect from exc


class CannotConnect(HomeAssistantError):
    """Error to indicate we cannot connect."""


class InvalidHost(HomeAssistantError):
    """Error to indicate there is invalid auth."""
