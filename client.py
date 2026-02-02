"""Client for the durable KV store over TCP."""

import json
import socket
from typing import Any, Optional

from protocol import decode_response, encode_request


class KVClient:
    """Client for the key-value store. Methods: Get(key), Set(key, value), Delete(key), BulkSet([(key, value)])."""

    def __init__(self, host: str = "127.0.0.1", port: int = 9999, timeout: float = 10.0):
        self._host = host
        self._port = port
        self._timeout = timeout

    def _request(self, method: str, **kwargs: Any) -> dict:
        """Send request and return decoded response."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self._timeout)
        try:
            sock.connect((self._host, self._port))
            sock.sendall(encode_request(method, **kwargs))
            buffer = b""
            while b"\n" not in buffer:
                chunk = sock.recv(4096)
                if not chunk:
                    raise ConnectionError("Connection closed")
                buffer += chunk
            line, _ = buffer.split(b"\n", 1)
            return decode_response(line)
        finally:
            sock.close()

    def Get(self, key: str) -> Optional[Any]:
        """Get value for key. Returns None if key does not exist."""
        resp = self._request("get", key=key)
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "get failed"))
        return resp.get("value")

    def Set(
        self,
        key: str,
        value: Any,
        *,
        debug_simulate_fail: bool = False,
    ) -> None:
        """Set key to value."""
        resp = self._request("set", key=key, value=value, debug_simulate_fail=debug_simulate_fail)
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "set failed"))

    def Delete(self, key: str, *, debug_simulate_fail: bool = False) -> None:
        """Delete key."""
        resp = self._request("delete", key=key, debug_simulate_fail=debug_simulate_fail)
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "delete failed"))

    def BulkSet(
        self,
        items: list[tuple[str, Any]],
        *,
        debug_simulate_fail: bool = False,
    ) -> None:
        """Set multiple key-value pairs. items: list of (key, value) tuples."""
        if not items:
            return
        # JSON serializable list of [key, value]
        items_ser = [[k, v] for k, v in items]
        resp = self._request("bulk_set", items=items_ser, debug_simulate_fail=debug_simulate_fail)
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "bulk_set failed"))

    def Search(self, query: str) -> list[str]:
        """Full-text search on values. Returns list of keys. Requires server --enable-indexes."""
        resp = self._request("search", query=query)
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "search failed"))
        return resp.get("value", [])

    def SearchSimilar(self, query: str, top_k: int = 10) -> list[tuple[str, float]]:
        """Word-embedding similarity search. Returns [(key, score), ...]. Requires server --enable-indexes."""
        resp = self._request("search_similar", query=query, top_k=top_k)
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "search_similar failed"))
        return [tuple(x) for x in resp.get("value", [])]
