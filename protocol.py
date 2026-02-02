"""Simple line-based JSON protocol over TCP for KV store."""

import json
from typing import Any


def encode_request(method: str, **kwargs: Any) -> bytes:
    """Encode a request as a single JSON line."""
    msg = {"method": method, **kwargs}
    return (json.dumps(msg) + "\n").encode("utf-8")


def decode_request(data: bytes) -> dict:
    """Decode a request line."""
    return json.loads(data.decode("utf-8").strip())


def encode_response(ok: bool, value: Any = None, error: str | None = None) -> bytes:
    """Encode response as a single JSON line."""
    msg: dict = {"ok": ok}
    if value is not None:
        msg["value"] = value
    if error is not None:
        msg["error"] = error
    return (json.dumps(msg) + "\n").encode("utf-8")


def decode_response(data: bytes) -> dict:
    """Decode response line."""
    return json.loads(data.decode("utf-8").strip())
