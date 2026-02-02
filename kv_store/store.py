"""Core key-value store with Write-Ahead Log (WAL) and snapshot persistence."""

import json
import os
import random
import threading
from pathlib import Path
from typing import Any, Optional

from .indexes import FullTextIndex, WordEmbeddingIndex


class KVStore:
    """Durable key-value store. WAL is always synchronous; snapshot can simulate fsync failure."""

    def __init__(
        self,
        data_dir: str = "data",
        wal_filename: str = "wal.jsonl",
        snapshot_filename: str = "snapshot.json",
        debug_random_fail_chance: float = 0.0,
        enable_indexes: bool = False,
    ):
        self._data_dir = Path(data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._wal_path = self._data_dir / wal_filename
        self._snapshot_path = self._data_dir / snapshot_filename
        self._debug_random_fail_chance = debug_random_fail_chance
        self._data: dict[str, Any] = {}
        self._lock = threading.RLock()
        self._ft_index = FullTextIndex() if enable_indexes else None
        self._emb_index = WordEmbeddingIndex() if enable_indexes else None
        self._recover()

    def _recover(self) -> None:
        """Load from snapshot then replay WAL."""
        if self._snapshot_path.exists():
            try:
                with open(self._snapshot_path, "r") as f:
                    self._data = json.load(f)
            except (json.JSONDecodeError, OSError):
                self._data = {}
        else:
            self._data = {}

        if self._wal_path.exists():
            try:
                with open(self._wal_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        entry = json.loads(line)
                        op = entry.get("op")
                        key = entry.get("key")
                        if op == "set":
                            self._data[key] = entry.get("value")
                        elif op == "delete":
                            self._data.pop(key, None)
                        elif op == "bulk":
                            for k, v in entry.get("items", []):
                                self._data[k] = v
            except (json.JSONDecodeError, OSError):
                pass
        if self._ft_index or self._emb_index:
            for k, v in self._data.items():
                if self._ft_index:
                    self._ft_index.index_value(k, v)
                if self._emb_index:
                    self._emb_index.index_value(k, v)

    def _append_wal(self, entry: dict) -> None:
        """Append to WAL and fsync. Always synchronous - no random skip."""
        with open(self._wal_path, "a") as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _save_snapshot(self, debug_simulate_fail: bool = False) -> None:
        """Save full snapshot to disk. If debug_simulate_fail, may randomly skip (simulate fsync failure)."""
        if debug_simulate_fail and random.random() < self._debug_random_fail_chance:
            return
        with open(self._snapshot_path, "w") as f:
            json.dump(self._data, f)
            f.flush()
            os.fsync(f.fileno())

    def get(self, key: str) -> Optional[Any]:
        """Get value for key. Returns None if key does not exist."""
        with self._lock:
            return self._data.get(key)

    def set(
        self,
        key: str,
        value: Any,
        *,
        debug_simulate_fail: bool = False,
    ) -> None:
        """Set key to value. Persists via WAL (always) and optionally snapshot."""
        with self._lock:
            self._data[key] = value
            if self._ft_index:
                self._ft_index.index_value(key, value)
            if self._emb_index:
                self._emb_index.index_value(key, value)
            self._append_wal({"op": "set", "key": key, "value": value})
            self._save_snapshot(debug_simulate_fail=debug_simulate_fail)

    def delete(
        self,
        key: str,
        *,
        debug_simulate_fail: bool = False,
    ) -> None:
        """Delete key."""
        with self._lock:
            self._data.pop(key, None)
            if self._ft_index:
                self._ft_index.remove_key(key)
            if self._emb_index:
                self._emb_index.remove_key(key)
            self._append_wal({"op": "delete", "key": key})
            self._save_snapshot(debug_simulate_fail=debug_simulate_fail)

    def bulk_set(
        self,
        items: list[tuple[str, Any]],
        *,
        debug_simulate_fail: bool = False,
    ) -> None:
        """Set multiple key-value pairs atomically."""
        if not items:
            return
        with self._lock:
            for key, value in items:
                self._data[key] = value
                if self._ft_index:
                    self._ft_index.index_value(key, value)
                if self._emb_index:
                    self._emb_index.index_value(key, value)
            self._append_wal({"op": "bulk", "items": items})
            self._save_snapshot(debug_simulate_fail=debug_simulate_fail)

    def search_fulltext(self, query: str) -> list[str]:
        """Full-text search on values. Returns list of keys. Requires enable_indexes=True."""
        if not self._ft_index:
            return []
        with self._lock:
            return self._ft_index.search(query)

    def search_similar(self, query: str, top_k: int = 10) -> list[tuple[str, float]]:
        """Word-embedding similarity search. Returns [(key, score), ...]. Requires enable_indexes=True."""
        if not self._emb_index:
            return []
        with self._lock:
            return self._emb_index.search_similar(query, top_k=top_k)

    def snapshot_path(self) -> Path:
        return self._snapshot_path

    def wal_path(self) -> Path:
        return self._wal_path
