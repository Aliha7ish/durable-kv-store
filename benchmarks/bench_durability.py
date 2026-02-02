"""Durability benchmark: add data in one thread, kill DB randomly in another, restart and check acknowledged keys."""

import os
import signal
import subprocess
import sys
import threading
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
    import tempfile
    data_dir = tempfile.mkdtemp(prefix="kv_dura_")
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

    acknowledged: list[str] = []
    ack_lock = threading.Lock()
    stop_writer = threading.Event()
    stop_killer = threading.Event()

    def writer():
        client = KVClient(host="127.0.0.1", port=port, timeout=2.0)
        idx = 0
        while not stop_writer.is_set():
            key = f"k_{idx}"
            try:
                client.Set(key, f"v_{idx}")
                with ack_lock:
                    acknowledged.append(key)
                idx += 1
            except Exception:
                pass

    def killer():
        time.sleep(0.2)
        while not stop_killer.is_set():
            time.sleep(0.05 + (0.1 * (hash(threading.current_thread().ident) % 10) / 10))
            try:
                if sys.platform == "win32":
                    proc.kill()
                else:
                    os.kill(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, OSError):
                pass
            break

    t_w = threading.Thread(target=writer)
    t_k = threading.Thread(target=killer)
    t_w.start()
    t_k.start()
    t_k.join()
    time.sleep(0.2)
    stop_writer.set()
    t_w.join(timeout=2.0)

    # Restart server with same data_dir
    time.sleep(0.3)
    proc2 = subprocess.Popen(
        [sys.executable, "-m", "server", "--host", "127.0.0.1", "--port", str(port), "--data-dir", data_dir],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    time.sleep(0.5)
    client = KVClient(host="127.0.0.1", port=port, timeout=5.0)

    with ack_lock:
        ack_set = set(acknowledged)
    lost = []
    for key in ack_set:
        try:
            val = client.Get(key)
            if val is None:
                lost.append(key)
        except Exception:
            lost.append(key)
    total_ack = len(ack_set)
    total_lost = len(lost)
    pct = (100.0 * (total_ack - total_lost) / total_ack) if total_ack else 100.0
    print(f"Acknowledged: {total_ack}, After restart lost: {total_lost}, Durability: {pct:.1f}%")
    if lost and len(lost) <= 20:
        print("Lost keys (sample):", lost[:20])
    proc2.terminate()
    proc2.wait(timeout=2)
    print("Done.")


if __name__ == "__main__":
    main()
