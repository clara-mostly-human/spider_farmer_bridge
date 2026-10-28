"""Home Assistant side of the bridge app connection.

``RemoteProxy`` stands in for ``MITMProxy`` when the proxy runs in the
Spider Farmer Bridge app: the bus, services and options flow call it exactly
as they call the in-process proxy, and it carries those calls to the app over
the app's WebSocket API. Messages from the app are fed into the real
``SfBus``, so entities, services and the cards work unchanged.
"""
from __future__ import annotations

import asyncio
import itertools
import logging
from typing import Any

import aiohttp

from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from ..entity_defs import HA_STATUS_TOPIC, _mac
from . import protocol

_LOGGER = logging.getLogger(__name__)

_RETRY_MIN_SEC = 1.0
_RETRY_MAX_SEC = 30.0
_REQUEST_TIMEOUT_SEC = 30.0

# Hooks whose argument at this index is the device's config dict. In process,
# the bus and the proxy share one dict per device; here one dict per device is
# kept and updated in place, so whatever the bus holds stays current.
_DEVICE_CFG_ARG = {
    "register_device": 0,
    "retype_device": 0,
    "register_soil": 2,
    "outlet_seen": 2,
    "blocks_seen": 2,
    "prune_blocks": 2,
}


class BridgeError(HomeAssistantError):
    """The bridge app is unreachable or refused a request."""


class RemoteProxy:
    """The proxy, running in the bridge app."""

    remote = True

    def __init__(self, hass: HomeAssistant, bus, url: str, settings: dict) -> None:
        self._hass = hass
        self._bus = bus
        self.url = url
        self._settings = dict(settings)
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._ids = itertools.count(1)
        self._pending: dict[int, asyncio.Future] = {}
        self._sessions: set[str] = set()
        self._strip_hosts: dict[str, str] = {}
        self._device_cfgs: dict[str, dict] = {}
        self.connected = asyncio.Event()

    # ── settings the integration sets on the proxy ───────────────────────

    def _setting(self, key: str, value: Any) -> None:
        if self._settings.get(key) == value:
            return
        self._settings[key] = value
        self._send_settings()

    @property
    def allow_control(self) -> bool:
        return bool(self._settings.get("allow_control", False))

    @allow_control.setter
    def allow_control(self, value: bool) -> None:
        self._setting("allow_control", bool(value))

    @property
    def block_cloud(self) -> bool:
        return bool(self._settings.get("block_cloud", False))

    @block_cloud.setter
    def block_cloud(self, value: bool) -> None:
        self._setting("block_cloud", bool(value))

    @property
    def offline_timeout(self) -> float:
        return float(self._settings.get("offline_timeout") or 0)

    @offline_timeout.setter
    def offline_timeout(self, value: float) -> None:
        self._setting("offline_timeout", float(value or 0))

    def _send_settings(self) -> None:
        settings = {
            **self._settings,
            "time_zone": self._hass.config.time_zone,
            "temperature_unit": self._hass.config.units.temperature_unit,
        }
        self._send({"type": "settings", "settings": settings})

    # ── session lookups, answered from the app's mirror ──────────────────

    def has_session(self, mac: str) -> bool:
        return mac in self._sessions or _mac(mac) in self._sessions

    def host_cb_mac_for_strip(self, strip_mac: str):
        return self._strip_hosts.get(strip_mac)

    def close_session(self, mac: str) -> bool:
        had = self.has_session(mac)
        self._fire("close_session", mac)
        return had

    def close_all_sessions(self) -> None:
        self._fire("close_all_sessions")

    # ── device writes ────────────────────────────────────────────────────

    async def handle_command(self, topic: str, value: Any) -> None:
        await self._request("handle_command", topic, value)

    async def write_se_schedule(self, mac: str, periods: list) -> bool:
        return await self._request("write_se_schedule", mac, periods)

    async def write_outlet_schedule(self, mac: str, n: int, periods: list) -> bool:
        return await self._request("write_outlet_schedule", mac, n, periods)

    async def write_outlet_config(self, mac: str, n: int, mode: str, config: dict) -> bool:
        return await self._request("write_outlet_config", mac, n, mode, config)

    async def write_alarm_settings(self, mac: str, settings: dict) -> bool:
        return await self._request("write_alarm_settings", mac, settings)

    async def write_plan(self, mac: str, stages: list, enabled) -> bool:
        return await self._request("write_plan", mac, stages, enabled)

    async def set_sensor_heating(self, mac: str, on) -> bool:
        return await self._request("set_sensor_heating", mac, on)

    async def reboot_device(self, mac: str) -> bool:
        return await self._request("reboot_device", mac)

    # ── transport ────────────────────────────────────────────────────────

    def _send(self, message: dict) -> bool:
        ws = self._ws
        if ws is None or ws.closed:
            return False
        self._hass.async_create_task(
            ws.send_str(protocol.encode(message)), eager_start=True)
        return True

    def _fire(self, method: str, *args: Any) -> None:
        if not self._send({"type": "request", "id": next(self._ids),
                           "method": method, "args": list(args)}):
            _LOGGER.warning("Bridge app not connected; %s not sent", method)

    async def _request(self, method: str, *args: Any) -> Any:
        if self._ws is None or self._ws.closed:
            raise BridgeError("Spider Farmer Bridge app is not connected")
        request_id = next(self._ids)
        future = self._hass.loop.create_future()
        self._pending[request_id] = future
        try:
            await self._ws.send_str(protocol.encode({
                "type": "request", "id": request_id,
                "method": method, "args": list(args)}))
            async with asyncio.timeout(_REQUEST_TIMEOUT_SEC):
                return await future
        finally:
            self._pending.pop(request_id, None)

    async def run(self) -> None:
        """Stay connected to the app until cancelled."""
        session = async_get_clientsession(self._hass)
        delay = _RETRY_MIN_SEC
        while True:
            try:
                async with session.ws_connect(
                    self.url, heartbeat=30, max_msg_size=16 * 1024 * 1024
                ) as ws:
                    delay = _RETRY_MIN_SEC
                    await self._serve(ws)
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, OSError, ValueError) as err:
                _LOGGER.debug("Bridge app connection failed: %s", err)
            finally:
                self._disconnected()
            await asyncio.sleep(delay)
            delay = min(delay * 2, _RETRY_MAX_SEC)

    async def _serve(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        hello = protocol.decode(await ws.receive_str())
        if hello.get("type") != "hello":
            raise ValueError("bridge app did not say hello")
        if hello.get("protocol") != protocol.PROTOCOL_VERSION:
            _LOGGER.error(
                "Spider Farmer Bridge app speaks protocol %s, this integration "
                "speaks %s; update the one that is behind",
                hello.get("protocol"), protocol.PROTOCOL_VERSION,
            )
            await asyncio.sleep(_RETRY_MAX_SEC)
            return
        self._ws = ws
        self._send_settings()
        self._on_sessions(hello)
        for item in hello.get("snapshot") or []:
            self._dispatch(item)
        self._bus.publish(HA_STATUS_TOPIC, "online")
        self.connected.set()
        _LOGGER.info(
            "Connected to the Spider Farmer Bridge app %s at %s",
            hello.get("version"), self.url,
        )
        async for msg in ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            try:
                self._dispatch(protocol.decode(msg.data))
            except Exception:  # noqa: BLE001 - one bad message must not drop the link
                _LOGGER.exception("Bridge app message failed")

    def _disconnected(self) -> None:
        was_connected = self._ws is not None
        self._ws = None
        self.connected.clear()
        for future in self._pending.values():
            if not future.done():
                future.set_exception(BridgeError("Bridge app disconnected"))
        self._pending.clear()
        if was_connected:
            _LOGGER.warning("Lost the Spider Farmer Bridge app; retrying")
            self._bus.publish(HA_STATUS_TOPIC, "offline")

    # ── messages from the app ────────────────────────────────────────────

    def _dispatch(self, message: dict) -> None:
        kind = message.get("type")
        if kind == "pub":
            self._on_publish(message)
        elif kind == "call":
            self._on_call(message.get("name"), list(message.get("args") or []))
        elif kind == "sessions":
            self._on_sessions(message)
        elif kind == "result":
            future = self._pending.get(message.get("id"))
            if future is not None and not future.done():
                if message.get("ok"):
                    future.set_result(message.get("value"))
                else:
                    future.set_exception(BridgeError(message.get("error")))

    def _on_publish(self, message: dict) -> None:
        bus = self._bus
        topic = message["topic"]
        value = message.get("value")
        for flag, mac in message.get("flags") or []:
            if flag == protocol.FLAG_AIR_SUPPRESSED:
                if bus.device_air_suppressed(mac, topic):
                    return
            elif flag == protocol.FLAG_LIGHT_LABEL:
                value = bus.light_mode_label_override(mac, topic, value)
            elif flag == protocol.FLAG_ENV_OUTLET_MODE:
                value = bus.env_outlet_mode_override(mac, topic, value)
        bus.publish(topic, value, retain=bool(message.get("retain")))

    def _on_call(self, name: str, args: list) -> None:
        if name not in protocol.FORWARDED_HOOKS:
            _LOGGER.debug("Ignoring unknown bus hook %s", name)
            return
        index = _DEVICE_CFG_ARG.get(name)
        if index is not None and len(args) > index and isinstance(args[index], dict):
            args[index] = self._device_cfg(args[index])
        if name == "set_outlet_mode_from_device" and len(args) >= 3:
            # In process the proxy applies this override before the call.
            args[2] = self._bus.env_outlet_modetype_override(args[0], args[1], args[2])
        hook = getattr(self._bus, name, None)
        if hook is not None:
            hook(*args)

    def _device_cfg(self, incoming: dict) -> dict:
        mac = _mac(str(incoming.get("mac", "")))
        cfg = self._device_cfgs.setdefault(mac, {})
        cfg.clear()
        cfg.update(incoming)
        return cfg

    def _on_sessions(self, message: dict) -> None:
        self._sessions = set(message.get("sessions") or [])
        self._strip_hosts = dict(message.get("strip_hosts") or {})
