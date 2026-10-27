"""Authenticated HTTP view + websocket commands for time-lapse frames.

Frames live in a private folder (never /www). The only way in is this view, which
requires a valid HA session OR a short-lived signed URL. The card asks the websocket
commands for the frame list, gets back SIGNED image URLs, and drops them into <img>.
No token is ever embedded in markup and the signatures expire.

Multi-instance (v3.19.362): a config entry can hold many time-lapses, one per camera,
each keyed by a slug (tl_id). Every URL and websocket command carries the tl_id so it
resolves to the right instance's private folder. When a command omits tl_id it falls
back to the FIRST instance (back-compat for the single-time-lapse era).
"""
from __future__ import annotations

from datetime import timedelta

import voluptuous as vol
from aiohttp import web

from homeassistant.core import HomeAssistant, callback
from homeassistant.components.http import HomeAssistantView
from homeassistant.components import websocket_api

from .const import DOMAIN
from .timelapse import list_sessions, list_frames, resolve_frame, resolve_video, read_env

FRAME_URL = "/api/sf_timelapse/{entry_id}/{tl_id}/{session}/{name}"
_SIGN_TTL = timedelta(minutes=10)


def _managers(hass: HomeAssistant):
    """Yield (entry_id, manager) for every entry that has a time-lapse manager."""
    for entry_id, data in (hass.data.get(DOMAIN) or {}).items():
        mgr = data.get("timelapse") if isinstance(data, dict) else None
        if mgr is not None:
            yield entry_id, mgr


def _resolve(hass: HomeAssistant, entry_id: str | None, tl_id: str | None):
    """Return (entry_id, controller) for the given keys. With no entry_id, search
    all entries; with no tl_id, use that manager's first instance."""
    for eid, mgr in _managers(hass):
        if entry_id and eid != entry_id:
            continue
        ctrl = mgr.get(tl_id)
        if ctrl is not None:
            return eid, ctrl
        if entry_id:  # asked for this entry specifically but it has no match
            return eid, None
    return None, None


class SfTimelapseFrameView(HomeAssistantView):
    """Serve a single frame's bytes from the private storage root.

    requires_auth stays True (default): a valid HA session OR a signed URL
    (async_sign_path) satisfies it. Path params are traversal-guarded.
    """

    url = "/api/sf_timelapse/{entry_id}/{tl_id}/{session}/{name}"
    name = "api:sf_timelapse:frame"

    async def get(self, request, entry_id, tl_id, session, name):
        hass = request.app["hass"]
        _eid, ctrl = _resolve(hass, entry_id, tl_id)
        if ctrl is None:
            return web.Response(status=404)
        is_video = name.lower().endswith(".mp4")
        resolver = resolve_video if is_video else resolve_frame
        path = await hass.async_add_executor_job(resolver, ctrl.root, session, name)
        if not path:
            return web.Response(status=404)
        # FileResponse streams + supports range requests (video seeking) and sets
        # the content-type from the extension.
        return web.FileResponse(path, headers={"Cache-Control": "max-age=3600"})


@callback
def async_register_timelapse_http(hass: HomeAssistant) -> None:
    """Register the frame view + websocket commands once. Resilient: if the http
    component isn't available yet (minimal/test harness), skip silently so entry
    setup never fails over it — production always has http by this point."""
    if hass.data.get("sf_timelapse_http_registered"):
        return
    http = getattr(hass, "http", None)
    if http is None:
        return
    try:
        http.register_view(SfTimelapseFrameView())
        websocket_api.async_register_command(hass, ws_list)
        websocket_api.async_register_command(hass, ws_sessions)
        websocket_api.async_register_command(hass, ws_frames)
        websocket_api.async_register_command(hass, ws_export_url)
        hass.data["sf_timelapse_http_registered"] = True
    except Exception:  # pragma: no cover - never break setup over the viewer
        pass


def _signed_frame_url(hass, entry_id, tl_id, session, name) -> str:
    from homeassistant.components.http.auth import async_sign_path
    path = FRAME_URL.format(entry_id=entry_id, tl_id=tl_id, session=session, name=name)
    return async_sign_path(hass, path, _SIGN_TTL)


@websocket_api.websocket_command({vol.Required("type"): "sf/timelapse/list"})
@callback
def ws_list(hass, connection, msg):
    """All configured time-lapse instances (so the card/editor can discover them)."""
    instances = []
    for entry_id, mgr in _managers(hass):
        for tl_id in mgr.ids():
            ctrl = mgr.get(tl_id)
            if ctrl is None:
                continue
            instances.append({
                "entry_id": entry_id,
                "tl_id": tl_id,
                "name": ctrl.name,
                "label": ctrl.label,
                "camera": ctrl.camera,
                "status": ctrl.status(),
            })
    connection.send_result(msg["id"], {"instances": instances})


@websocket_api.websocket_command({
    vol.Required("type"): "sf/timelapse/sessions",
    vol.Optional("tl_id"): str,
})
@callback
def ws_sessions(hass, connection, msg):
    entry_id, ctrl = _resolve(hass, None, msg.get("tl_id"))
    if ctrl is None:
        connection.send_result(msg["id"], {"sessions": [], "status": None})
        return

    async def _run():
        sessions = await hass.async_add_executor_job(list_sessions, ctrl.root)
        connection.send_result(msg["id"], {
            "entry_id": entry_id,
            "tl_id": ctrl.tl_id,
            "status": ctrl.status(),
            "sessions": sessions,
        })

    hass.async_create_task(_run())


@websocket_api.websocket_command({
    vol.Required("type"): "sf/timelapse/frames",
    vol.Optional("tl_id"): str,
    vol.Optional("session"): str,
})
@callback
def ws_frames(hass, connection, msg):
    entry_id, ctrl = _resolve(hass, None, msg.get("tl_id"))
    if ctrl is None:
        connection.send_result(msg["id"], {"frames": []})
        return
    session = msg.get("session") or (ctrl.status() or {}).get("session")

    async def _run():
        if not session:
            connection.send_result(msg["id"], {"session": None, "frames": []})
            return
        names = await hass.async_add_executor_job(list_frames, ctrl.root, session)
        env = await hass.async_add_executor_job(read_env, ctrl.root, session)
        frames = [{
            "name": n,
            "stamp": n[:-4],
            "url": _signed_frame_url(hass, entry_id, ctrl.tl_id, session, n),
            "data": env.get(n[:-4]),
        } for n in names]
        connection.send_result(msg["id"], {
            "tl_id": ctrl.tl_id, "session": session, "frames": frames})

    hass.async_create_task(_run())


@websocket_api.websocket_command({
    vol.Required("type"): "sf/timelapse/export_url",
    vol.Optional("tl_id"): str,
    vol.Optional("session"): str,
    vol.Optional("name"): str,
})
@callback
def ws_export_url(hass, connection, msg):
    """Signed, expiring URL for an exported .mp4 (defaults to the last export)."""
    entry_id, ctrl = _resolve(hass, None, msg.get("tl_id"))
    if ctrl is None:
        connection.send_result(msg["id"], {"url": None})
        return
    le = (ctrl.status() or {}).get("last_export") or {}
    session = msg.get("session") or le.get("session")
    name = msg.get("name") or le.get("name")
    if not (session and name):
        connection.send_result(msg["id"], {"url": None})
        return
    connection.send_result(msg["id"], {
        "tl_id": ctrl.tl_id, "session": session, "name": name,
        "url": _signed_frame_url(hass, entry_id, ctrl.tl_id, session, name),
    })
