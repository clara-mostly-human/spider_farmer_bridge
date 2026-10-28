"""A fake GGS controller for testing the bridge without hardware.

Connects to a proxy over TLS the way a controller does (trusting the bundled
device CA, server name sf.mqtt.spider-farmer.com), subscribes to its command
topic, reports a display-panel status frame every few seconds and prints every
command it receives.

Run it only against a proxy in local-only mode (Block cloud on): in relay
mode the proxy would forward the fake frames to the real Spider Farmer cloud.

    python -m bridge.fake_controller --port 8883 [--mac 0A1B2C3D4E01]
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import ssl

CERTS = Path(__file__).resolve().parent.parent / "custom_components" / "sf" / "certs"

FRAME = {
    "sensor": {"temp": 24.5, "humi": 61.0, "co2": 850, "vpd": 1.1, "ppfd": 400},
    "light": {"mOnOff": 1, "mLevel": 80},
    "blower": {"mOnOff": 1, "mLevel": 65, "modeType": 1},
    "fan": {"mOnOff": 1, "mLevel": 7, "shakeLevel": 3, "natural": 0, "modeType": 0},
}


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value % 128
        value //= 128
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)


def _string(text: str) -> bytes:
    data = text.encode()
    return len(data).to_bytes(2, "big") + data


def connect_packet(client_id: str) -> bytes:
    body = _string("MQTT") + b"\x04\x02\x00\x3c" + _string(client_id)
    return b"\x10" + _varint(len(body)) + body


def subscribe_packet(topic: str, packet_id: int = 1) -> bytes:
    body = packet_id.to_bytes(2, "big") + _string(topic) + b"\x00"
    return b"\x82" + _varint(len(body)) + body


def publish_packet(topic: str, message: dict) -> bytes:
    body = _string(topic) + json.dumps(message).encode()
    return b"\x30" + _varint(len(body)) + body


async def _read_packets(reader: asyncio.StreamReader, on_command=None) -> None:
    buf = b""
    while True:
        chunk = await reader.read(4096)
        if not chunk:
            print("connection closed", flush=True)
            return
        buf += chunk
        while len(buf) >= 2:
            length, shift, i = 0, 0, 1
            while True:
                if i >= len(buf):
                    break
                byte = buf[i]
                length |= (byte & 0x7F) << shift
                shift += 7
                i += 1
                if not byte & 0x80:
                    break
            else:
                break
            if len(buf) < i + length:
                break
            header, body = buf[0], buf[i:i + length]
            ptype, qos = header >> 4, (header >> 1) & 0x03
            buf = buf[i + length:]
            if ptype == 3:
                tlen = int.from_bytes(body[:2], "big")
                topic = body[2:2 + tlen].decode()
                payload = body[2 + tlen + (2 if qos else 0):]
                text = payload.decode(errors="replace")
                if on_command is not None:
                    on_command(topic, text)
                print(f"command on {topic}: {text}", flush=True)
            else:
                print(f"packet type {ptype}", flush=True)


async def run(args: argparse.Namespace, on_command=None) -> None:
    ctx = ssl.create_default_context(cafile=str(CERTS / "ca.pem"))
    # The vendor certificate has no Authority Key Identifier, which Python's
    # strict X.509 mode (default since 3.13) rejects; the controllers don't.
    ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
    reader, writer = await asyncio.open_connection(
        args.host, args.port, ssl=ctx, server_hostname="sf.mqtt.spider-farmer.com")
    writer.write(connect_packet(args.mac))
    writer.write(subscribe_packet(f"SF/GGS/CB/API/DOWN/{args.mac}"))
    await writer.drain()
    reading = asyncio.ensure_future(_read_packets(reader, on_command))
    for n in range(args.count):
        frame = json.loads(json.dumps(FRAME))
        frame["sensor"]["temp"] = round(24.5 + n * 0.1, 1)
        writer.write(publish_packet(
            f"SF/GGS/CB/API/UP/{args.mac}",
            {"method": "getDevSta", "uid": "fake", "data": frame}))
        try:
            await writer.drain()
        except ConnectionError:
            print("connection lost", flush=True)
            return
        print(f"sent status {n + 1}/{args.count}", flush=True)
        await asyncio.sleep(args.interval)
    if args.linger:
        await reading   # stay connected, silent, until the proxy hangs up
    reading.cancel()
    writer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8883)
    parser.add_argument("--mac", default="0A1B2C3D4E01")
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--count", type=int, default=1000000)
    parser.add_argument("--linger", action="store_true",
                        help="stay connected after the last frame")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
