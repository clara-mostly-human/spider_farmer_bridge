"""Wire protocol between the bridge app and the integration.

Pure Python, no Home Assistant imports: the bridge app loads this module next
to the proxy.

The app runs ``proxy/`` unchanged. Everything the proxy hands to the state bus
(``publish`` plus the bus hooks it looks up with ``getattr``) travels to the
integration as a message, and the integration calls the real ``SfBus`` with it.
Everything the integration asks of the proxy (writes, reboot, settings,
session control) travels the other way as a request.

One WebSocket text frame carries one JSON message with a ``type``:

app -> integration
    hello     protocol, bridge version, snapshot (replayed calls and retained
              publishes, in order), sessions
    pub       topic, value, retain, flags (bus value hooks to apply first)
    call      name, args: one bus hook call
    sessions  sessions (live session keys), strip_hosts {strip mac: panel mac}
    result    id, ok, value | error: reply to a request

integration -> app
    settings  allow_control, block_cloud, offline_timeout, time_zone,
              temperature_unit
    request   id, method, args
"""

from __future__ import annotations

import json
from typing import Any

PROTOCOL_VERSION = 1

# Bus hooks the proxy calls for their side effects only (none of their return
# values is used); the app forwards each call to the integration.
FORWARDED_HOOKS = frozenset({
    "register_device",
    "register_soil",
    "prune_outlet",
    "outlet_seen",
    "blocks_seen",
    "prune_blocks",
    "retype_device",
    "set_outlet_mode_from_device",
    "maybe_adopt_env_outlet",
    "note_soil_type",
    "apply_oplog",
    "apply_alarms",
    "apply_soil_labels",
    "apply_plan_progress",
    "apply_plan",
    "apply_alarm_settings",
    "apply_air_calibration",
})

# Bus hooks that rewrite or drop a value right before the proxy publishes it.
# They read integration-side state, so the app answers "unchanged" and flags
# the publish; the integration applies the real hook on arrival.
FLAG_AIR_SUPPRESSED = "air_suppressed"      # device_air_suppressed
FLAG_LIGHT_LABEL = "light_label"            # light_mode_label_override
FLAG_ENV_OUTLET_MODE = "env_outlet_mode"    # env_outlet_mode_override

# Proxy methods the integration may call.
REQUEST_METHODS = frozenset({
    "handle_command",
    "write_se_schedule",
    "write_outlet_schedule",
    "write_outlet_config",
    "write_alarm_settings",
    "write_plan",
    "set_sensor_heating",
    "reboot_device",
    "close_session",
    "close_all_sessions",
})

# apply_alarms / apply_oplog carry events, not state: the app keeps this many
# recent calls per device for replay instead of only the last one.
EVENT_HOOKS = frozenset({"apply_alarms", "apply_oplog"})
EVENT_REPLAY_LIMIT = 200


def _to_wire(value: Any) -> Any:
    if isinstance(value, set | frozenset):
        return {"__set__": [_to_wire(v) for v in value]}
    if isinstance(value, tuple):
        return {"__tuple__": [_to_wire(v) for v in value]}
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(k): _to_wire(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_wire(v) for v in value]
    return value


def _from_wire(value: Any) -> Any:
    if isinstance(value, dict):
        if len(value) == 1 and "__set__" in value:
            return {_from_wire(v) for v in value["__set__"]}
        if len(value) == 1 and "__tuple__" in value:
            return tuple(_from_wire(v) for v in value["__tuple__"])
        return {k: _from_wire(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_from_wire(v) for v in value]
    return value


def encode(message: dict) -> str:
    """Serialize one message (sets and tuples survive the round trip)."""
    return json.dumps(_to_wire(message), separators=(",", ":"))


def decode(text: str) -> dict:
    """Parse one message."""
    message = _from_wire(json.loads(text))
    if not isinstance(message, dict) or "type" not in message:
        raise ValueError("not a bridge message")
    return message


def replay_key(name: str, args: list) -> tuple:
    """Identity of a hook call for replay: a later call with the same key
    replaces the earlier one (the bus hooks are idempotent per key)."""
    if name in ("register_device", "retype_device"):
        cfg = args[0] if args and isinstance(args[0], dict) else {}
        return ("device", str(cfg.get("mac", "")).lower())
    if name in ("outlet_seen", "prune_outlet", "set_outlet_mode_from_device",
                "maybe_adopt_env_outlet", "register_soil"):
        return (name, str(args[0]).lower(), str(args[1]) if len(args) > 1 else "")
    if name == "note_soil_type":
        return (name, str(args[0]) if args else "")
    return (name, str(args[0]).lower() if args else "")
