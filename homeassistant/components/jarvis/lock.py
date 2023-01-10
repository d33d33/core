"""Platform for light integration."""
from __future__ import annotations

import asyncio
from enum import Enum
import json
import logging
from urllib.parse import urljoin

import httpx

# Import the device class from the component that you want to support
from homeassistant.components.lock import LockEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


class Status(Enum):
    """lock status."""

    Unlocked = 0
    Locked = 1
    Locking = 2
    Unlocking = 3
    Jammed = 4
    Error = 5


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_devices: AddEntitiesCallback
) -> None:
    """Set up entry."""
    locks = []
    locks.append(JarvisLockEntity("entree", entry.data))

    async_add_devices(locks)


class JarvisLockEntity(LockEntity):
    """Jarvis lock."""

    should_poll = False

    def __init__(self, name, config) -> None:
        """Initialize an JarvisLockEntity."""
        self._config = config
        self._endpoint = urljoin(config["host"], "/door")
        self._name = name
        self._unique_id = f"jarvis.lock.{name}"
        self._state = Status.Error
        self._loop = asyncio.get_event_loop()
        self._loop.create_task(self.state_poller())

    @property
    def name(self) -> str:
        """Return the display name of this lock."""
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
            model="lock",
        )

    @property
    def is_locking(self) -> bool | None:
        """Return true if lock is locking."""
        return self._state == Status.Locking

    @property
    def is_locked(self) -> bool | None:
        """Return true if lock is locked."""
        return self._state == Status.Locked

    @property
    def is_jammed(self) -> bool | None:
        """Return true if lock is jammed."""
        return self._state == Status.Jammed

    @property
    def is_unlocking(self) -> bool | None:
        """Return true if lock is unlocking."""
        return self._state == Status.Unlocking

    @property
    def available(self) -> bool:
        """Return True if entity is available."""
        return self._state != Status.Error

    @property
    def icon(self) -> str | None:
        """Icon of the entity."""
        return "mdi:lock"

    async def async_lock(self, **kwargs):
        """Lock all or specified locks. A code to lock the lock with may optionally be specified."""
        async with httpx.AsyncClient() as client:
            try:
                res = await client.put(urljoin(self._endpoint, "/door/lock"))
                res.raise_for_status()
                self._state = Status.Locking
            except httpx.RequestError as exc:
                self._state = Status.Error
                _LOGGER.warning("An error occurred while parsing request %s", exc)
            except Exception as exc:
                self._state = Status.Error
                _LOGGER.error("Unexpected %s, %s", exc, type(exc))
                raise exc

            self.async_schedule_update_ha_state()

    async def async_unlock(self, **kwargs):
        """Unlock all or specified locks. A code to unlock the lock with may optionally be specified."""
        async with httpx.AsyncClient() as client:
            try:
                res = await client.put(urljoin(self._endpoint, "/door/unlock"))
                res.raise_for_status()
                self._state = Status.Locking
            except httpx.RequestError as exc:
                self._state = Status.Error
                _LOGGER.warning("An error occurred while parsing request %s", exc)
            except Exception as exc:
                self._state = Status.Error
                _LOGGER.error("Unexpected %s, %s", exc, type(exc))
                raise exc

            self.async_schedule_update_ha_state()

    async def state_poller(self) -> None:
        """Poll door state."""
        while True:
            state = Status.Error
            async with httpx.AsyncClient() as client:
                try:
                    res = await client.get(self._endpoint)
                    res.raise_for_status()

                    r_state = res.json()["state"]
                    if r_state == "locked":
                        state = Status.Locked
                    if r_state == "unlocked":
                        state = Status.Unlocked
                    if r_state == "jammed":
                        state = Status.Jammed
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
