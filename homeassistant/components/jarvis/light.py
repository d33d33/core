"""Platform for light integration."""
from __future__ import annotations

import asyncio
from enum import Enum
import json
import logging
from urllib.parse import urljoin

import httpx

# Import the device class from the component that you want to support
from homeassistant.components.light import LightEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


class Status(Enum):
    """Light status."""

    On = 0
    Off = 1
    Error = 2


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_devices: AddEntitiesCallback
) -> None:
    """Set up entry."""
    lights = []
    lights.append(JarvisLightEntity("jardin", entry.data))
    lights.append(JarvisLightEntity("guirlande", entry.data))

    async_add_devices(lights)


class JarvisLightEntity(LightEntity):
    """Jarvis light."""

    should_poll = False

    def __init__(self, name, config) -> None:
        """Initialize an JarvisLightEntity."""
        self._config = config
        self._endpoint = urljoin(config["host"], f"/light/{name}")
        self._name = name
        self._unique_id = f"jarvis.light.{name}"
        self._state = Status.Error
        self._loop = asyncio.get_event_loop()
        self._loop.create_task(self.state_poller())

    @property
    def name(self) -> str:
        """Return the display name of this light."""
        return self._name

    @property
    def unique_id(self) -> str | None:
        """Return a unique ID."""
        return self._unique_id

    @property
    def device_info(self) -> DeviceInfo:
        """Return the device info."""
        assert self.unique_id is not None
        return DeviceInfo(
            identifiers={
                # Serial numbers are unique identifiers within a specific domain
                (DOMAIN, self.unique_id)
            },
            name=self.name,
            manufacturer="d33d33",
            model="light",
        )

    @property
    def available(self) -> bool:
        """Return True if entity is available."""
        return self._state != Status.Error

    @property
    def is_on(self) -> bool | None:
        """Return true if light is on."""
        return self._state == Status.On

    async def async_turn_on(self, **kwargs):
        """Turn light on."""
        async with httpx.AsyncClient() as client:
            try:
                res = await client.put(
                    urljoin(self._endpoint, f"/light/{self._name}/on")
                )
                res.raise_for_status()
                self._state = Status.On
            except httpx.RequestError as exc:
                self._state = Status.Error
                _LOGGER.warning("An error occurred while parsing request %s", exc)
            except Exception as exc:
                self._state = Status.Error
                _LOGGER.error("Unexpected %s, %s", exc, type(exc))
                raise exc

            self.async_schedule_update_ha_state()

    async def async_turn_off(self, **kwargs):
        """Turn light off."""
        async with httpx.AsyncClient() as client:
            try:
                res = await client.put(
                    urljoin(self._endpoint, f"/light/{self._name}/off")
                )
                res.raise_for_status()
                self._state = Status.Off
            except httpx.RequestError as exc:
                self._state = Status.Error
                _LOGGER.warning("An error occurred while parsing request %s", exc)
            except Exception as exc:
                self._state = Status.Error
                _LOGGER.error("Unexpected %s, %s", exc, type(exc))
                raise exc

            self.async_schedule_update_ha_state()

    async def state_poller(self) -> None:
        """Poll light state."""
        while True:
            state = Status.Error
            async with httpx.AsyncClient() as client:
                try:
                    res = await client.get(self._endpoint)
                    res.raise_for_status()

                    r_state = res.json()["state"]
                    if r_state == "on":
                        state = Status.On
                    if r_state == "off":
                        state = Status.Off
                    if r_state == "unknown":
                        state = Status.Error

                except httpx.RequestError as exc:
                    state = Status.Error
                    _LOGGER.warning(
                        "An error occurred while requesting %s", exc.request.url
                    )
                except json.JSONDecodeError as exc:
                    state = Status.Error
                    _LOGGER.warning(
                        "An error occurred while parsing request %s", exc.msg
                    )
                except Exception as exc:
                    state = Status.Error
                    _LOGGER.error("Unexpected %s, %s", exc, type(exc))
                    raise exc

            if self._state != state:
                self._state = state
                self.async_schedule_update_ha_state()
            await asyncio.sleep(1)
