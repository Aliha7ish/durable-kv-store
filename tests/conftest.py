"""Pytest fixtures: start/stop server subprocess, client on free port."""

import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

# Add project root to path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from client import KVClient


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def data_dir(tmp_path):
    return str(tmp_path / "kv_data")


@pytest.fixture
def port():
    return find_free_port()


@pytest.fixture
def server_process(data_dir, port):
    """Start server as subprocess; yield; then terminate gracefully."""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    proc = subprocess.Popen(
        [sys.executable, "-m", "server", "--host", "127.0.0.1", "--port", str(port), "--data-dir", data_dir],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    time.sleep(0.3)  # allow bind
    yield proc
    proc.terminate()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
    proc.wait()


@pytest.fixture
def client(port):
    return KVClient(host="127.0.0.1", port=port, timeout=5.0)
