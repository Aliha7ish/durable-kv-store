"""Client that discovers primary in a 3-node cluster and forwards requests to it."""

import json
import socket
import time
from typing import Any, Optional

from protocol import decode_response, encode_request


class ClusterKVClient:
    """KV client for cluster: discovers primary among kv_ports, then uses it for Get/Set/Delete/BulkSet."""

    def __init__(self, kv_ports: list[int], host: str = "127.0.0.1", timeout: float = 10.0):
        self._host = host
        self._kv_ports = list(kv_ports)
        self._timeout = timeout
        self._primary_port: Optional[int] = None

    def _discover_primary(self) -> int:
        """Find which port is primary by asking 'role'. Retries a few times for startup."""
        for attempt in range(10):
            for port in self._kv_ports:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(2.0)
                    s.connect((self._host, port))
                    s.sendall(b'{"method":"role"}\n')
                    buf = b""
                    while b"\n" not in buf:
                        buf += s.recv(4096)
                        if not buf:
                            break
                    s.close()
                    if b"\n" in buf:
                        line, _ = buf.split(b"\n", 1)
                        r = json.loads(line.decode("utf-8"))
                        if r.get("primary"):
                            return port
                except Exception:
                    continue
            time.sleep(0.5)
        raise RuntimeError("No primary found in cluster")

    def _get_primary_port(self) -> int:
        if self._primary_port is None:
            self._primary_port = self._discover_primary()
        return self._primary_port

    def _request(self, method: str, **kwargs: Any) -> dict:
        port = self._get_primary_port()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self._timeout)
        try:
            sock.connect((self._host, port))
            sock.sendall(encode_request(method, **kwargs))
            buffer = b""
            while b"\n" not in buffer:
                chunk = sock.recv(4096)
                if not chunk:
                    raise ConnectionError("Connection closed")
                buffer += chunk
            line, _ = buffer.split(b"\n", 1)
            resp = decode_response(line)
            if not resp.get("ok") and "not primary" in str(resp.get("error", "")):
                self._primary_port = None
                return self._request(method, **kwargs)
            return resp
        finally:
            sock.close()

    def Get(self, key: str) -> Optional[Any]:
        resp = self._request("get", key=key)
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "get failed"))
        return resp.get("value")

    def Set(self, key: str, value: Any, *, debug_simulate_fail: bool = False) -> None:
        resp = self._request("set", key=key, value=value, debug_simulate_fail=debug_simulate_fail)
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "set failed"))

    def Delete(self, key: str, *, debug_simulate_fail: bool = False) -> None:
        resp = self._request("delete", key=key, debug_simulate_fail=debug_simulate_fail)
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "delete failed"))

    def BulkSet(self, items: list[tuple[str, Any]], *, debug_simulate_fail: bool = False) -> None:
        if not items:
            return
        items_ser = [[k, v] for k, v in items]
        resp = self._request("bulk_set", items=items_ser, debug_simulate_fail=debug_simulate_fail)
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "bulk_set failed"))
