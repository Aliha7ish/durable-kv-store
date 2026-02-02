"""Replication: primary replicates WAL to secondaries; election when primary goes down."""

import json
import socket
import threading
import time
from pathlib import Path
from typing import Any, Callable, Optional

from kv_store import KVStore


def _send_line(sock: socket.socket, data: bytes) -> None:
    sock.sendall(data + b"\n")


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


class ReplicationReceiver:
    """Receives WAL entries from primary and applies them to local store."""

    def __init__(self, store: KVStore, port: int):
        self._store = store
        self._port = port
        self._sock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    def start(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", self._port))
        self._sock.listen(2)
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def _apply_entry(self, entry: dict) -> None:
        op = entry.get("op")
        if op == "set":
            self._store._data[entry["key"]] = entry["value"]
        elif op == "delete":
            self._store._data.pop(entry.get("key"), None)
        elif op == "bulk":
            for k, v in entry.get("items", []):
                self._store._data[k] = v

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            self._sock.settimeout(0.5)
            try:
                conn, _ = self._sock.accept()
            except (socket.timeout, OSError):
                continue
            try:
                while not self._stop.is_set():
                    line = _recv_line(conn, timeout=1.0)
                    entry = json.loads(line.decode("utf-8"))
                    with self._store._lock:
                        self._apply_entry(entry)
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


class ReplicationSender:
    """Sends WAL entries to secondaries."""

    def __init__(self, secondary_peers: list[tuple[str, int]]):
        self._peers = list(secondary_peers)
        self._connections: list[socket.socket] = []
        self._lock = threading.Lock()

    def replicate(self, entry: dict) -> None:
        line = (json.dumps(entry) + "\n").encode("utf-8")
        with self._lock:
            dead = []
            for i, conn in enumerate(self._connections):
                try:
                    conn.sendall(line)
                except OSError:
                    dead.append(i)
            for i in reversed(dead):
                try:
                    self._connections[i].close()
                except OSError:
                    pass
                del self._connections[i]

    def connect_to_peers(self) -> None:
        with self._lock:
            for host, port in self._peers:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(2.0)
                    s.connect((host, port))
                    self._connections.append(s)
                except OSError:
                    pass

    def close(self) -> None:
        with self._lock:
            for c in self._connections:
                try:
                    c.close()
                except OSError:
                    pass
            self._connections.clear()


class ClusterNode:
    """Single node: either primary or secondary. Role can change via election."""

    def __init__(
        self,
        node_id: int,
        kv_port: int,
        repl_port: int,
        peer_repl_ports: list[int],
        data_dir: str,
        peer_kv_ports: list[int],
    ):
        self.node_id = node_id
        self.kv_port = kv_port
        self.repl_port = repl_port
        self.peer_repl_ports = peer_repl_ports  # replication ports of other nodes (secondaries when we're primary)
        self.peer_kv_ports = peer_kv_ports  # kv ports of other nodes (for election / discovery)
        self.data_dir = data_dir
        self._store = KVStore(data_dir=data_dir)
        self._is_primary = node_id == 0  # node 0 starts as primary
        self._receiver: Optional[ReplicationReceiver] = None
        self._sender: Optional[ReplicationSender] = None
        self._lock = threading.Lock()
        self._heartbeat_received = threading.Event()
        self._heartbeat_received.set()
        self._election_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    @property
    def store(self) -> KVStore:
        return self._store

    def _replicate(self, entry: dict) -> None:
        if self._sender:
            self._sender.replicate(entry)

    def _start_primary(self) -> None:
        peers = [("127.0.0.1", p) for p in self.peer_repl_ports]
        self._sender = ReplicationSender(peers)
        self._sender.connect_to_peers()

    def _start_secondary(self) -> None:
        self._receiver = ReplicationReceiver(self._store, self.repl_port)
        self._receiver.start()

    def _run_election(self) -> None:
        """When we think primary is down, contact peers; lowest live node_id becomes primary."""
        time.sleep(0.5)
        with self._lock:
            if self._is_primary:
                return
        # Ask other secondaries: who has the lowest id?
        my_id = self.node_id
        for kv_port in self.peer_kv_ports:
            if kv_port == self.kv_port:
                continue
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(1.0)
                s.connect(("127.0.0.1", kv_port))
                s.sendall(b'{"method":"role"}\n')
                line = _recv_line(s, timeout=1.0)
                s.close()
                resp = json.loads(line.decode("utf-8"))
                if resp.get("primary"):
                    return  # someone else is primary
                other_id = resp.get("node_id", 999)
                if other_id < my_id:
                    return  # they have lower id, they'll become primary
            except Exception:
                pass
        # I have the lowest id among reachable; become primary
        with self._lock:
            if self._is_primary:
                return
            self._become_primary()

    def _become_primary(self) -> None:
        if self._receiver:
            self._receiver.stop()
            self._receiver = None
        self._is_primary = True
        self._start_primary()

    def start(self) -> None:
        if self._is_primary:
            self._start_primary()
        else:
            self._start_secondary()
            self._election_thread = threading.Thread(target=self._election_loop, daemon=True)
            self._election_thread.start()

    def _election_loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(1.0)
            with self._lock:
                if self._is_primary:
                    continue
            self._run_election()

    def stop(self) -> None:
        self._stop.set()
        if self._sender:
            self._sender.close()
        if self._receiver:
            self._receiver.stop()

    def is_primary(self) -> bool:
        return self._is_primary

    def apply_and_replicate(self, entry: dict) -> None:
        with self._lock:
            if not self._is_primary:
                raise RuntimeError("not primary")
            self._store._data  # ensure store is updated in same order as WAL
            if entry.get("op") == "set":
                self._store._data[entry["key"]] = entry["value"]
            elif entry.get("op") == "delete":
                self._store._data.pop(entry.get("key"), None)
            elif entry.get("op") == "bulk":
                for k, v in entry.get("items", []):
                    self._store._data[k] = v
            self._replicate(entry)
