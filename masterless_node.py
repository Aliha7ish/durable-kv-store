"""Master-less replication: all nodes accept reads and writes; replicate to all peers (last-writer-wins)."""

import json
import socket
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kv_store import KVStore
from protocol import encode_response


def _recv_line(sock: socket.socket, timeout: float = 2.0) -> bytes:
    sock.settimeout(timeout)
    buf = b""
    while b"\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("closed")
        buf += chunk
    line, _ = buf.split(b"\n", 1)
    return line


class ReplicationBroadcaster:
    """Send WAL entries to all peer nodes (master-less: every node replicates to others)."""

    def __init__(self, peer_repl_ports: list[int]):
        self._peers = [("127.0.0.1", p) for p in peer_repl_ports]
        self._conns: list[socket.socket] = []
        self._lock = threading.Lock()

    def connect(self) -> None:
        with self._lock:
            for host, port in self._peers:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(2.0)
                    s.connect((host, port))
                    self._conns.append(s)
                except OSError:
                    pass

    def broadcast(self, entry: dict) -> None:
        line = (json.dumps(entry) + "\n").encode("utf-8")
        with self._lock:
            dead = []
            for i, c in enumerate(self._conns):
                try:
                    c.sendall(line)
                except OSError:
                    dead.append(i)
            for i in reversed(dead):
                try:
                    self._conns[i].close()
                except OSError:
                    pass
                del self._conns[i]

    def close(self) -> None:
        with self._lock:
            for c in self._conns:
                try:
                    c.close()
                except OSError:
                    pass
            self._conns.clear()


class ReplicationReceiver(threading.Thread):
    """Receive WAL entries from peers and apply to store (master-less)."""

    def __init__(self, store: KVStore, repl_port: int):
        super().__init__(daemon=True)
        self._store = store
        self._port = repl_port
        self._stop = threading.Event()
        self._sock: socket.socket | None = None

    def run(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", self._port))
        self._sock.listen(4)
        self._sock.settimeout(0.5)
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except (socket.timeout, OSError):
                continue
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn: socket.socket) -> None:
        try:
            while not self._stop.is_set():
                line = _recv_line(conn, timeout=1.0)
                entry = json.loads(line.decode("utf-8"))
                with self._store._lock:
                    op = entry.get("op")
                    if op == "set":
                        self._store._data[entry["key"]] = entry["value"]
                    elif op == "delete":
                        self._store._data.pop(entry.get("key"), None)
                    elif op == "bulk":
                        for k, v in entry.get("items", []):
                            self._store._data[k] = v
                    self._store._append_wal(entry)
        except (ConnectionError, json.JSONDecodeError, OSError):
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def stop(self) -> None:
        self._stop.set()
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass


def run_masterless_node(
    node_id: int,
    kv_port: int,
    repl_port: int,
    peer_repl_ports: list[int],
    data_dir: str,
) -> None:
    """Run one master-less node: accept reads/writes and replicate to peers."""
    store = KVStore(data_dir=data_dir)
    receiver = ReplicationReceiver(store, repl_port)
    receiver.start()
    broadcaster = ReplicationBroadcaster(peer_repl_ports)
    broadcaster.connect()

    def apply_and_broadcast(entry: dict) -> None:
        with store._lock:
            op = entry.get("op")
            if op == "set":
                store._data[entry["key"]] = entry["value"]
            elif op == "delete":
                store._data.pop(entry.get("key"), None)
            elif op == "bulk":
                for k, v in entry.get("items", []):
                    store._data[k] = v
            store._append_wal(entry)
            store._save_snapshot(debug_simulate_fail=False)
        broadcaster.broadcast(entry)

    def handle_client(conn: socket.socket) -> None:
        buffer = b""
        try:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                buffer += data
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    if not line:
                        continue
                    try:
                        req = json.loads(line.decode("utf-8").strip())
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        conn.sendall(encode_response(False, error="invalid request"))
                        continue
                    method = req.get("method")
                    if method == "get":
                        key = req.get("key")
                        if key is None:
                            conn.sendall(encode_response(False, error="missing key"))
                            continue
                        value = store.get(key)
                        conn.sendall(encode_response(True, value=value))
                    elif method == "set":
                        key, value = req.get("key"), req.get("value")
                        if key is None:
                            conn.sendall(encode_response(False, error="missing key"))
                            continue
                        entry = {"op": "set", "key": key, "value": value}
                        apply_and_broadcast(entry)
                        conn.sendall(encode_response(True))
                    elif method == "delete":
                        key = req.get("key")
                        if key is None:
                            conn.sendall(encode_response(False, error="missing key"))
                            continue
                        entry = {"op": "delete", "key": key}
                        apply_and_broadcast(entry)
                        conn.sendall(encode_response(True))
                    elif method == "bulk_set":
                        raw = req.get("items", [])
                        items = [tuple(pair) for pair in raw]
                        if not items:
                            conn.sendall(encode_response(True))
                            continue
                        entry = {"op": "bulk", "items": items}
                        apply_and_broadcast(entry)
                        conn.sendall(encode_response(True))
                    else:
                        conn.sendall(encode_response(False, error=f"unknown method: {method}"))
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            conn.close()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", kv_port))
    server.listen(64)
    try:
        while True:
            conn, _ = server.accept()
            threading.Thread(target=handle_client, args=(conn,), daemon=True).start()
    finally:
        server.close()
        broadcaster.close()
        receiver.stop()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--node-id", type=int, required=True)
    ap.add_argument("--kv-port", type=int, required=True)
    ap.add_argument("--repl-port", type=int, required=True)
    ap.add_argument("--peer-repl-ports", type=int, nargs="+", required=True)
    ap.add_argument("--data-dir", required=True)
    args = ap.parse_args()
    run_masterless_node(
        node_id=args.node_id,
        kv_port=args.kv_port,
        repl_port=args.repl_port,
        peer_repl_ports=args.peer_repl_ports,
        data_dir=args.data_dir,
    )
