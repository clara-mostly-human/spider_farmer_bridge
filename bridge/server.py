"""Spider Farmer Bridge server: the proxy as its own process.

Runs ``custom_components/sf/proxy`` unchanged, outside Home Assistant, and
serves the integration over a WebSocket (``custom_components/sf/remote``), the
way Matter Server serves Home Assistant's Matter integration. The proxy keeps
the controllers' TLS sessions (and the cloud relay) while Home Assistant
restarts; on reconnect the integration gets a snapshot of everything so far.

Usage:
    python -m bridge.server --data DIR [--device-port 8883] [--api-port 8484]
        [--api-host 127.0.0.1] [--upstream-host HOST] [--upstream-port 8883]
"""

from __future__ import annotations

import argparse
import asyncio
from collections import OrderedDict, deque
import importlib.machinery
import importlib.util
import json
import logging
import os
from pathlib import Path
import signal
import ssl
import sys
from types import SimpleNamespace
from typing import Any

from aiohttp import WSMsgType, web

_LOGGER = logging.getLogger("sf_bridge")

CORE_DIR = Path(__file__).resolve().parent.parent / "custom_components" / "sf"
CORE_PACKAGE = "sf_core"

# Same cadence as the integration's in-process setup.
_POLL_INTERVAL_SEC = 600
_SESSIONS_INTERVAL_SEC = 1.0


def load_core():
    """Import the integration's proxy layer without its Home Assistant parts.

    Importing ``custom_components.sf.proxy`` would run ``sf/__init__.py``,
    which needs Home Assistant. A bare package pointing at the same folder
    lets the proxy's relative imports (``..tempunits``, ``..ha.discovery``)
    resolve to the HA-free modules next to it.
    """
    if CORE_PACKAGE not in sys.modules:
        spec = importlib.machinery.ModuleSpec(CORE_PACKAGE, None, is_package=True)
        package = importlib.util.module_from_spec(spec)
        package.__path__ = [str(CORE_DIR)]
        sys.modules[CORE_PACKAGE] = package
    mitm_proxy = importlib.import_module(f"{CORE_PACKAGE}.proxy.mitm_proxy")
    return SimpleNamespace(
        MITMProxy=mitm_proxy.MITMProxy,
        cert_manager=importlib.import_module(f"{CORE_PACKAGE}.cert_manager"),
        tempunits=importlib.import_module(f"{CORE_PACKAGE}.tempunits"),
        protocol=importlib.import_module(f"{CORE_PACKAGE}.remote.protocol"),
        version=json.loads((CORE_DIR / "manifest.json").read_text())["version"],
    )


class AppBus:
    """Stands in for the integration's ``SfBus`` inside the app.

    Publishes and side-effect hooks go to the hub, which forwards them to the
    integration. The value hooks need integration-side state, so they answer
    "unchanged" here and flag the publish that follows for the integration to
    apply them.
    """

    def __init__(self, hub: Hub) -> None:
        self._hub = hub
        self._flags: dict[str, list] = {}
        self.proxy = None
        # build_tz_sync_command reads hass.config.time_zone.
        self.hass = SimpleNamespace(config=SimpleNamespace(time_zone=None))

    def publish(self, topic: str, payload: Any, retain: bool = False, qos: int = 0) -> None:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8", errors="replace")
        self._hub.on_publish(topic, payload, retain, self._flags.pop(topic, []))

    def cached(self, topic: str):
        return self._hub.cached_state(topic)

    def _flag(self, topic: str, flag: str, mac: str) -> None:
        self._flags.setdefault(topic, []).append([flag, mac])

    def device_air_suppressed(self, mac_raw: str, topic: str) -> bool:
        self._flag(topic, self._hub.protocol.FLAG_AIR_SUPPRESSED, mac_raw)
        return False

    def light_mode_label_override(self, mac_raw: str, topic: str, val: str) -> str:
        self._flag(topic, self._hub.protocol.FLAG_LIGHT_LABEL, mac_raw)
        return val

    def env_outlet_mode_override(self, mac: str, topic: str, val: str) -> str:
        self._flag(topic, self._hub.protocol.FLAG_ENV_OUTLET_MODE, mac)
        return val

    def __getattr__(self, name: str):
        if name in self._hub.protocol.FORWARDED_HOOKS:
            return lambda *args: self._hub.on_call(name, list(args))
        raise AttributeError(name)

    # MQTT-client surface the proxy may touch.
    def subscribe(self, *a, **kw): pass
    def unsubscribe(self, *a, **kw): pass
    def loop_start(self): pass
    def loop_stop(self): pass


class Hub:
    """WebSocket API: fans proxy output out to clients, runs their requests."""

    def __init__(self, core, data_dir: Path) -> None:
        self.protocol = core.protocol
        self._core = core
        self._settings_file = data_dir / "settings.json"
        self._clients: set[web.WebSocketResponse] = set()
        # Retained publishes: topic -> (value, flags), oldest first.
        self._retained: OrderedDict[str, tuple[Any, list]] = OrderedDict()
        # Replay log of hook calls: key -> (name, args), oldest first.
        self._calls: OrderedDict[tuple, tuple[str, list]] = OrderedDict()
        self._events: dict[tuple, deque] = {}
        self._sessions: dict = {"sessions": [], "strip_hosts": {}}
        self.bus = AppBus(self)
        self.proxy = None

    # ── proxy -> clients ─────────────────────────────────────────────────

    def on_publish(self, topic: str, value: Any, retain: bool, flags: list) -> None:
        self._check_sessions()
        if retain:
            self._retained[topic] = (value, flags)
            self._retained.move_to_end(topic)
        self._broadcast({"type": "pub", "topic": topic, "value": value,
                         "retain": retain, "flags": flags})

    def on_call(self, name: str, args: list) -> None:
        self._check_sessions()
        self._record(name, args)
        self._broadcast({"type": "call", "name": name, "args": args})

    def cached_state(self, topic: str):
        entry = self._retained.get(topic)
        return None if entry is None else str(entry[0])

    def _record(self, name: str, args: list) -> None:
        protocol = self.protocol
        if name in protocol.EVENT_HOOKS:
            key = protocol.replay_key(name, args)
            log = self._events.setdefault(
                key, deque(maxlen=protocol.EVENT_REPLAY_LIMIT))
            log.append(args)
            self._calls[key] = (name, None)
            self._calls.move_to_end(key)
            return
        # A replayed retype would drop the device's slot again; what the
        # integration needs on reconnect is the device as it is now.
        replay_name = "register_device" if name == "retype_device" else name
        key = protocol.replay_key(replay_name, args)
        self._calls[key] = (replay_name, args)
        self._calls.move_to_end(key)

    def _snapshot(self) -> list:
        items: list = []
        for key, (name, args) in self._calls.items():
            if args is None:
                items.extend({"type": "call", "name": name, "args": a}
                             for a in self._events.get(key, ()))
            else:
                items.append({"type": "call", "name": name, "args": args})
        items.extend({"type": "pub", "topic": t, "value": v, "retain": True,
                      "flags": f} for t, (v, f) in self._retained.items())
        return items

    def _broadcast(self, message: dict) -> None:
        if not self._clients:
            return
        text = self.protocol.encode(message)
        for ws in list(self._clients):
            asyncio.ensure_future(self._send(ws, text))

    @staticmethod
    async def _send(ws: web.WebSocketResponse, text: str) -> None:
        try:
            await ws.send_str(text)
        except (ConnectionResetError, RuntimeError):
            pass

    def _check_sessions(self) -> None:
        """Mirror the proxy's live sessions for has_session/host lookups.

        Checked before every message, so the integration learns of a new
        session before that session's first state arrives."""
        current = self._session_state()
        if current != self._sessions:
            self._sessions = current
            self._broadcast({"type": "sessions", **current})

    async def sessions_loop(self) -> None:
        """Catch session changes that send nothing (a dropped controller)."""
        while True:
            await asyncio.sleep(_SESSIONS_INTERVAL_SEC)
            self._check_sessions()

    def _session_state(self) -> dict:
        proxy = self.proxy
        sessions = sorted(proxy._sessions) if proxy else []
        hosts = {}
        for key in sessions:
            sess = proxy._sessions.get(key)
            mac = getattr(sess, "mac", None)
            if not mac:
                continue
            host = proxy.host_cb_mac_for_strip(mac)
            if host:
                hosts[mac] = host
        return {"sessions": sessions, "strip_hosts": hosts}

    # ── settings ─────────────────────────────────────────────────────────

    def load_settings(self) -> None:
        try:
            settings = json.loads(self._settings_file.read_text())
        except (OSError, ValueError):
            return
        self.apply_settings(settings, persist=False)

    def apply_settings(self, settings: dict, persist: bool = True) -> None:
        proxy = self.proxy
        if "allow_control" in settings:
            proxy.allow_control = bool(settings["allow_control"])
        if "block_cloud" in settings:
            proxy.block_cloud = bool(settings["block_cloud"])
        if "offline_timeout" in settings:
            proxy.offline_timeout = float(settings["offline_timeout"] or 0)
        if settings.get("temperature_unit"):
            self._core.tempunits.set_unit(settings["temperature_unit"])
        if settings.get("time_zone"):
            self.bus.hass.config.time_zone = settings["time_zone"]
        _LOGGER.info(
            "Settings: control %s, cloud %s, offline timeout %ss, unit %s, tz %s",
            "allowed" if proxy.allow_control else "blocked",
            "blocked (local only)" if proxy.block_cloud else "relayed",
            proxy.offline_timeout, self._core.tempunits.unit(),
            self.bus.hass.config.time_zone,
        )
        if persist:
            tmp = self._settings_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(settings))
            tmp.replace(self._settings_file)

    # ── WebSocket endpoint ───────────────────────────────────────────────

    async def handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=30, max_msg_size=16 * 1024 * 1024)
        await ws.prepare(request)
        _LOGGER.info("Client connected from %s", request.remote)
        await ws.send_str(self.protocol.encode({
            "type": "hello",
            "protocol": self.protocol.PROTOCOL_VERSION,
            "version": self._core.version,
            "snapshot": self._snapshot(),
            **self._sessions,
        }))
        self._clients.add(ws)
        try:
            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    continue
                try:
                    message = self.protocol.decode(msg.data)
                except ValueError:
                    _LOGGER.warning("Ignoring a malformed message")
                    continue
                if message["type"] == "settings":
                    self.apply_settings(message.get("settings") or {})
                elif message["type"] == "request":
                    asyncio.ensure_future(self._run_request(ws, message))
        finally:
            self._clients.discard(ws)
            _LOGGER.info("Client disconnected")
        return ws

    async def _run_request(self, ws: web.WebSocketResponse, message: dict) -> None:
        method = message.get("method")
        reply: dict = {"type": "result", "id": message.get("id")}
        if method not in self.protocol.REQUEST_METHODS:
            reply.update(ok=False, error=f"unknown method {method!r}")
        else:
            try:
                value = getattr(self.proxy, method)(*(message.get("args") or []))
                if asyncio.iscoroutine(value):
                    value = await value
                reply.update(ok=True, value=value)
            except Exception as err:  # noqa: BLE001 - reported to the caller
                _LOGGER.exception("Request %s failed", method)
                reply.update(ok=False, error=str(err))
        await self._send(ws, self.protocol.encode(reply))


class Bridge:
    """The running bridge: device listener, proxy loops and API."""

    def __init__(self, hub: Hub, device_server, runner, tasks) -> None:
        self.hub = hub
        self._device_server = device_server
        self._runner = runner
        self._tasks = tasks

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()
        self.hub.proxy.close_all_sessions()
        self._device_server.close()
        await self._device_server.wait_closed()
        await self._runner.cleanup()


async def start(
    data_dir: Path,
    *,
    device_port: int = 8883,
    api_host: str = "127.0.0.1",
    api_port: int = 8484,
    upstream_host: str = "sf.mqtt.spider-farmer.com",
    upstream_port: int = 8883,
) -> Bridge:
    core = load_core()
    data_dir.mkdir(parents=True, exist_ok=True)

    hub = Hub(core, data_dir)
    cert_dir = core.cert_manager.ensure_certs(str(data_dir))
    server_cert, server_key, ca_cert = core.cert_manager.cert_paths(cert_dir)
    proxy = core.MITMProxy(
        config={"proxy": {
            "listen_host": "0.0.0.0",
            "listen_port": device_port,
            "upstream_host": upstream_host,
            "upstream_port": upstream_port,
            "cert_file": server_cert,
            "upstream_ca_file": core.cert_manager.upstream_ca_path(),
            "key_file": server_key,
            "ca_file": ca_cert,
            "config_poll_interval_sec": _POLL_INTERVAL_SEC,
        }},
        mqtt_client=hub.bus,
        config_path=None,
    )
    hub.proxy = proxy
    hub.bus.proxy = proxy
    hub.load_settings()

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(certfile=server_cert, keyfile=server_key)
    device_server = await asyncio.start_server(
        proxy.handle_client, host="0.0.0.0", port=device_port,
        ssl=ctx, reuse_address=True,
    )
    _LOGGER.info("Listening for controllers on port %d", device_port)

    app = web.Application()
    app.router.add_get("/ws", hub.handle_ws)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, api_host, api_port).start()
    _LOGGER.info("API on ws://%s:%d/ws (bridge %s, protocol %d)",
                 api_host, api_port, core.version,
                 core.protocol.PROTOCOL_VERSION)

    tasks = [
        asyncio.ensure_future(proxy.config_poll_loop()),
        asyncio.ensure_future(proxy.offline_sweep_loop()),
        asyncio.ensure_future(hub.sessions_loop()),
    ]
    return Bridge(hub, device_server, runner, tasks)


async def run(args: argparse.Namespace) -> None:
    bridge = await start(
        Path(args.data),
        device_port=args.device_port,
        api_host=args.api_host,
        api_port=args.api_port,
        upstream_host=args.upstream_host,
        upstream_port=args.upstream_port,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    _LOGGER.info("Stopping")
    await bridge.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Spider Farmer Bridge server")
    parser.add_argument("--data", required=True, help="folder for settings")
    parser.add_argument("--device-port", type=int, default=8883)
    parser.add_argument("--api-host", default="127.0.0.1")
    parser.add_argument("--api-port", type=int, default=8484)
    parser.add_argument("--upstream-host", default="sf.mqtt.spider-farmer.com")
    parser.add_argument("--upstream-port", type=int, default=8883)
    parser.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "info"))
    args = parser.parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(run(args))
    return 0


if __name__ == "__main__":
    sys.exit(main())
