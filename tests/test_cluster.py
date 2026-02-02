"""Tests for cluster: replication and election when primary goes down."""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from cluster_client import ClusterKVClient


def _free_port():
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start_node(node_id: int, kv_port: int, repl_port: int, secondary_repl_ports: list[int], data_dir: str, other_kv_ports: list[int]) -> subprocess.Popen:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    cmd = [
        sys.executable, "-m", "cluster_node",
        "--node-id", str(node_id),
        "--kv-port", str(kv_port),
        "--repl-port", str(repl_port),
        "--secondary-repl-ports", *map(str, secondary_repl_ports),
        "--data-dir", data_dir,
        "--other-kv-ports", *map(str, other_kv_ports),
    ]
    return subprocess.Popen(cmd, cwd=str(ROOT), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def _free_ports(n: int, base: int = 0) -> list[int]:
    import socket
    ports = []
    for _ in range(n):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            ports.append(s.getsockname()[1])
    return ports


@pytest.fixture
def cluster_ports(tmp_path):
    """Three nodes: dynamic kv and repl ports."""
    kv_ports = _free_ports(3)
    repl_ports = _free_ports(3)
    return kv_ports, repl_ports


@pytest.fixture
def cluster(cluster_ports, tmp_path):
    kv_ports, repl_ports = cluster_ports
    data_dirs = [str(tmp_path / f"node{i}") for i in range(3)]
    procs = []
    for i in range(3):
        other_kv = [p for j, p in enumerate(kv_ports) if j != i]
        sec_repl = [repl_ports[j] for j in range(3) if j != 0]
        if i != 0:
            sec_repl = [repl_ports[j] for j in range(3) if j != i]
        proc = _start_node(i, kv_ports[i], repl_ports[i], sec_repl, data_dirs[i], other_kv)
        procs.append(proc)
    time.sleep(3.0)
    yield kv_ports
    for p in procs:
        p.terminate()
        try:
            p.wait(timeout=2)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()


class TestClusterReplicationAndElection:
    """Writes go to primary; kill primary; one secondary becomes primary and serves."""

    def test_write_then_read_via_primary(self, cluster):
        client = ClusterKVClient(kv_ports=cluster, timeout=5.0)
        client.Set("k1", "v1")
        assert client.Get("k1") == "v1"

    def test_kill_primary_then_new_primary_serves(self, cluster_ports, tmp_path):
        kv_ports, repl_ports = cluster_ports
        data_dirs = [str(tmp_path / f"node{i}") for i in range(3)]
        procs = []
        for i in range(3):
            other_kv = [p for j, p in enumerate(kv_ports) if j != i]
            sec_repl = [repl_ports[j] for j in range(3) if j != i]
            proc = _start_node(i, kv_ports[i], repl_ports[i], sec_repl, data_dirs[i], other_kv)
            procs.append(proc)
        time.sleep(1.5)
        client = ClusterKVClient(kv_ports=kv_ports, timeout=5.0)
        client.Set("before", "value")
        assert client.Get("before") == "value"
        primary_proc = procs[0]
        primary_proc.kill()
        if sys.platform != "win32":
            try:
                import signal as sig
                try:
                    os.kill(primary_proc.pid, sig.SIGKILL)
                except ProcessLookupError:
                    pass
            except Exception:
                pass
        primary_proc.wait(timeout=2)
        time.sleep(3.0)
        client._primary_port = None
        assert client.Get("before") == "value"
        client.Set("after", "value2")
        assert client.Get("after") == "value2"
        for p in procs[1:]:
            p.terminate()
            try:
                p.wait(timeout=2)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
