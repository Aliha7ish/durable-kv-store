"""Test debug_simulate_fail: random skip of snapshot (not WAL) to simulate fsync failure."""

import os
import subprocess
import sys
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


def _start_server(data_dir: str, port: int, debug_fail_chance: float = 0.0) -> subprocess.Popen:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    cmd = [
        sys.executable, "-m", "server",
        "--host", "127.0.0.1", "--port", str(port),
        "--data-dir", data_dir,
        "--debug-fail-chance", str(debug_fail_chance),
    ]
    return subprocess.Popen(cmd, cwd=str(ROOT), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def _stop_gracefully(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


class TestDebugSimulateFail:
    """With debug_simulate_fail, snapshot may be skipped; WAL is always applied. After restart, data from WAL is intact."""

    def test_with_debug_fail_restart_still_has_data(self, tmp_path):
        """Server started with --debug-fail-chance; set values; restart; all acknowledged keys should be present (WAL)."""
        data_dir = str(tmp_path / "debug_data")
        port = _free_port()
        proc = _start_server(data_dir, port, debug_fail_chance=0.5)  # 50% skip snapshot
        time.sleep(0.4)
        client = KVClient(host="127.0.0.1", port=port, timeout=5.0)
        keys_set = []
        for i in range(30):
            key = f"dk_{i}"
            client.Set(key, f"v_{i}", debug_simulate_fail=True)
            keys_set.append(key)
        _stop_gracefully(proc)
        time.sleep(0.2)
        proc2 = _start_server(data_dir, port, debug_fail_chance=0.0)
        time.sleep(0.4)
        client2 = KVClient(host="127.0.0.1", port=port, timeout=5.0)
        for key in keys_set:
            assert client2.Get(key) is not None, f"Key {key} lost after restart (WAL should preserve)"
        _stop_gracefully(proc2)
