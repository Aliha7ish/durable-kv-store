"""ACID tests: concurrent bulk set same keys; bulk write + kill server (all or nothing)."""

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from client import KVClient


def _free_port():
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start_server(data_dir: str, port: int) -> subprocess.Popen:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    return subprocess.Popen(
        [sys.executable, "-m", "server", "--host", "127.0.0.1", "--port", str(port), "--data-dir", data_dir],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


def _kill_server(proc: subprocess.Popen) -> None:
    try:
        if sys.platform == "win32":
            proc.kill()
        else:
            os.kill(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, OSError):
        pass


class TestConcurrentBulkSetSameKeys:
    """Concurrent bulk set touching the same keys - should not corrupt (last write wins, no partial state)."""

    def test_concurrent_bulk_set_same_keys(self, server_process, client, port):
        """Multiple threads do BulkSet that touch overlapping keys; after join, each key has one consistent value."""
        results: list[Exception | None] = []
        keys_shared = [f"shared_{i}" for i in range(20)]

        def bulk_setter(thread_id: int, n: int):
            try:
                c = KVClient(host="127.0.0.1", port=port, timeout=10.0)
                for i in range(n):
                    items = [(k, f"t{thread_id}_{i}") for k in keys_shared]
                    c.BulkSet(items)
                results.append(None)
            except Exception as e:
                results.append(e)

        threads = [
            threading.Thread(target=bulk_setter, args=(tid, 15))
            for tid in range(3)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for r in results:
            assert r is None, r
        # Each key should have *some* value of form t{X}_{Y} (no mixed/corrupt state)
        for k in keys_shared:
            val = client.Get(k)
            assert val is not None
            assert isinstance(val, str)
            assert val.startswith("t") and "_" in val


class TestBulkSetAllOrNothingOnKill:
    """Bulk write + kill server randomly: bulk should be either fully applied or not (no partial)."""

    @pytest.mark.timeout(30)
    def test_bulk_set_then_kill_consistency(self, tmp_path):
        """Run writer that does BulkSets; killer kills during run; restart and check no partial bulk."""
        data_dir = str(tmp_path / "acid_data")
        port = _free_port()
        proc = _start_server(data_dir, port)
        time.sleep(0.4)
        client = KVClient(host="127.0.0.1", port=port, timeout=5.0)
        batch_size = 50
        completed_batches: list[list[tuple[str, str]]] = []
        lock = threading.Lock()
        stop = threading.Event()

        def writer():
            idx = 0
            while not stop.is_set():
                items = [(f"bulk_{idx}_{j}", f"v_{idx}_{j}") for j in range(batch_size)]
                try:
                    client.BulkSet(items)
                    with lock:
                        completed_batches.append(items)
                    idx += 1
                except Exception:
                    pass

        def killer():
            time.sleep(0.3)
            _kill_server(proc)

        t_w = threading.Thread(target=writer)
        t_k = threading.Thread(target=killer)
        t_w.start()
        t_k.start()
        t_k.join()
        time.sleep(0.2)
        stop.set()
        t_w.join(timeout=2)

        # Restart and verify: for each completed batch, either ALL keys present or NONE (all-or-nothing)
        time.sleep(0.3)
        proc2 = _start_server(data_dir, port)
        time.sleep(0.4)
        client2 = KVClient(host="127.0.0.1", port=port, timeout=5.0)
        partial_batches = 0
        for items in completed_batches:
            present = sum(1 for (k, _) in items if client2.Get(k) is not None)
            if 0 < present < len(items):
                partial_batches += 1
        proc2.terminate()
        try:
            proc2.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc2.kill()
            proc2.wait()
        assert partial_batches == 0, f"Found {partial_batches} partially applied bulk batches"
