"""Write throughput benchmark: measure writes/sec with pre-populated data at different sizes."""

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from client import KVClient


def free_port():
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    data_dir = tempfile.mkdtemp(prefix="kv_bench_")
    port = free_port()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    proc = subprocess.Popen(
        [sys.executable, "-m", "server", "--host", "127.0.0.1", "--port", str(port), "--data-dir", data_dir],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    time.sleep(0.5)
    client = KVClient(host="127.0.0.1", port=port, timeout=30.0)

    # Pre-populate at different sizes, then measure write throughput
    for n_pre in [0, 1_000, 10_000, 50_000]:
        if n_pre > 0:
            batch = 500
            for i in range(0, n_pre, batch):
                items = [(f"pre_{j}", f"value_{j}") for j in range(i, min(i + batch, n_pre))]
                client.BulkSet(items)
        # Measure write throughput: 2000 single writes
        n_writes = 2000
        start = time.perf_counter()
        for i in range(n_writes):
            client.Set(f"w_{n_pre}_{i}", f"v_{i}")
        elapsed = time.perf_counter() - start
        writes_per_sec = n_writes / elapsed
        print(f"Pre-populated {n_pre:>6} keys -> {writes_per_sec:>8.0f} writes/sec ({n_writes} writes in {elapsed:.2f}s)")

    proc.terminate()
    proc.wait(timeout=3)
    print("Done.")


if __name__ == "__main__":
    main()
