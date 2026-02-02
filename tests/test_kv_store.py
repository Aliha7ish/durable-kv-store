"""Tests for KV store: Set/Get, Delete, Get without set, Set twice, restart durability."""

import os
import signal
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


def _start_server(data_dir: str, port: int, **kwargs) -> subprocess.Popen:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    cmd = [sys.executable, "-m", "server", "--host", "127.0.0.1", "--port", str(port), "--data-dir", data_dir]
    if kwargs.get("debug_fail_chance") is not None:
        cmd.extend(["--debug-fail-chance", str(kwargs["debug_fail_chance"])])
    return subprocess.Popen(cmd, cwd=str(ROOT), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def _stop_gracefully(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


class TestSetThenGet:
    """Set then Get."""

    def test_set_then_get(self, server_process, client):
        client.Set("foo", "bar")
        assert client.Get("foo") == "bar"

    def test_set_then_get_bulk(self, server_process, client):
        client.BulkSet([("a", 1), ("b", 2), ("c", "three")])
        assert client.Get("a") == 1
        assert client.Get("b") == 2
        assert client.Get("c") == "three"


class TestSetThenDeleteThenGet:
    """Set then Delete then Get."""

    def test_set_then_delete_then_get(self, server_process, client):
        client.Set("x", "y")
        assert client.Get("x") == "y"
        client.Delete("x")
        assert client.Get("x") is None


class TestGetWithoutSetting:
    """Get without setting."""

    def test_get_missing_key(self, server_process, client):
        assert client.Get("nonexistent") is None


class TestSetThenSetThenGet:
    """Set then Set (same key) then Get."""

    def test_set_twice_same_key(self, server_process, client):
        client.Set("k", "v1")
        assert client.Get("k") == "v1"
        client.Set("k", "v2")
        assert client.Get("k") == "v2"


class TestSetThenExitThenGet:
    """Set then exit (gracefully) then Get - persistence across restarts."""

    def test_persistence_after_graceful_exit(self, tmp_path, port):
        data_dir = str(tmp_path / "kv_data")
        proc = _start_server(data_dir, port)
        try:
            time.sleep(0.4)
            c = KVClient(host="127.0.0.1", port=port, timeout=5.0)
            c.Set("persistent", "value_after_restart")
        finally:
            _stop_gracefully(proc)
        # Restart server with same data_dir
        proc2 = _start_server(data_dir, port)
        try:
            time.sleep(0.4)
            c2 = KVClient(host="127.0.0.1", port=port, timeout=5.0)
            assert c2.Get("persistent") == "value_after_restart"
        finally:
            _stop_gracefully(proc2)
