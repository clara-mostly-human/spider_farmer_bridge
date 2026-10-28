"""The integration as a client of the bridge app (proxy in its own process).

Runs the real bridge server and a fake controller over TLS on localhost. The
bridge's upstream points at a closed local port, so the fake controller is
never relayed to the Spider Farmer cloud.
"""
import argparse
import asyncio
import json

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry
from homeassistant.const import STATE_UNAVAILABLE
from homeassistant.core import HomeAssistant

from bridge import fake_controller
from bridge import server as bridge_server
from custom_components.sf.const import DOMAIN, DATA_PROXY

DEVICE_PORT = 28883
API_PORT = 28484
MAC = "0A1B2C3D4E01"
MAC_LC = MAC.lower()


@pytest.fixture(autouse=True)
def _e(enable_custom_integrations):
    yield


@pytest.fixture
async def bridge(tmp_path):
    started = await bridge_server.start(
        tmp_path / "bridge",
        device_port=DEVICE_PORT,
        api_port=API_PORT,
        upstream_host="127.0.0.1",
        upstream_port=9,   # closed: never reach the real cloud
    )
    yield started
    await started.close()


async def _setup(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN, title="Spider Farmer Bridge", unique_id=DOMAIN,
        data={"bridge_url": f"ws://127.0.0.1:{API_PORT}/ws",
              "allow_control": True, "block_cloud": True},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    proxy = hass.data[DOMAIN][entry.entry_id][DATA_PROXY]
    async with asyncio.timeout(10):
        await proxy.connected.wait()
    return entry


async def _controller(commands: list, count: int = 3,
                      linger: bool = False) -> asyncio.Task:
    args = argparse.Namespace(host="127.0.0.1", port=DEVICE_PORT, mac=MAC,
                              interval=0.2, count=count, linger=linger)
    return asyncio.ensure_future(fake_controller.run(
        args, on_command=lambda topic, text: commands.append(json.loads(text))))


async def _wait_for(hass, predicate, timeout=10.0):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.05)
            await hass.async_block_till_done()


async def test_entities_and_commands_through_the_app(hass: HomeAssistant, bridge):
    entry = await _setup(hass)
    # Pushed from the entry's settings.
    await _wait_for(hass, lambda: bridge.hub.proxy.block_cloud)
    assert bridge.hub.proxy.allow_control is True

    commands: list = []
    task = await _controller(commands, count=20)
    await _wait_for(hass, lambda: hass.states.get("sensor.sf_dp1_temperature"))
    temp = hass.states.get("sensor.sf_dp1_temperature")
    assert float(temp.state) >= 24.5
    assert temp.attributes["unit_of_measurement"] == "°C"   # HA's unit, pushed
    assert hass.data[DOMAIN][entry.entry_id][DATA_PROXY].has_session(MAC_LC)

    # A command from HA reaches the controller as a device write.
    await _wait_for(hass, lambda: hass.states.get("light.sf_dp1_light_1"))
    await hass.services.async_call(
        "light", "turn_off", {"entity_id": "light.sf_dp1_light_1"}, blocking=True)
    await _wait_for(hass, lambda: any(
        c.get("method") == "setConfigField"
        and c["params"]["keyPath"] == ["device", "light"] for c in commands))
    write = next(c for c in commands if c.get("method") == "setConfigField")
    assert write["params"]["light"]["mOnOff"] == 0
    # The proxy then reads the module back to confirm (its scheduled poll).
    await _wait_for(hass, lambda: any(
        c.get("method") == "getConfigField"
        and c["params"]["keyPath"] == ["device", "light"]
        for c in commands[commands.index(write) + 1:]), timeout=15)

    task.cancel()
    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_reload_gets_snapshot_without_new_frames(hass: HomeAssistant, bridge):
    entry = await _setup(hass)
    commands: list = []
    task = await _controller(commands, count=3, linger=True)
    await _wait_for(hass, lambda: hass.states.get("sensor.sf_dp1_temperature"))
    await asyncio.sleep(1)   # last frame sent; the controller stays connected, quiet
    sessions_before = list(bridge.hub.proxy._sessions)

    # Reloading Home Assistant's side must not sever the controller, and the
    # new instance must rebuild its entities from the app's snapshot.
    assert await hass.config_entries.async_reload(entry.entry_id)
    proxy = hass.data[DOMAIN][entry.entry_id][DATA_PROXY]
    async with asyncio.timeout(10):
        await proxy.connected.wait()
    await hass.async_block_till_done()
    temp = hass.states.get("sensor.sf_dp1_temperature")
    assert temp is not None and temp.state != STATE_UNAVAILABLE
    assert float(temp.state) >= 24.5
    # Same controller session: the reload did not sever it.
    assert list(bridge.hub.proxy._sessions) == sessions_before == [MAC_LC]

    task.cancel()
    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_entities_go_unavailable_when_the_app_stops(hass: HomeAssistant, tmp_path):
    started = await bridge_server.start(
        tmp_path / "bridge", device_port=DEVICE_PORT, api_port=API_PORT,
        upstream_host="127.0.0.1", upstream_port=9)
    entry = await _setup(hass)
    commands: list = []
    task = await _controller(commands, count=50)
    await _wait_for(hass, lambda: hass.states.get("sensor.sf_dp1_temperature"))

    await started.close()
    task.cancel()
    await _wait_for(hass, lambda: hass.states.get(
        "sensor.sf_dp1_temperature").state == STATE_UNAVAILABLE)

    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
