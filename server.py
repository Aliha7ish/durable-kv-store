"""TCP server for the durable KV store."""

import json
import socket
import threading

from kv_store import KVStore
from protocol import decode_request, encode_response


def handle_client(conn: socket.socket, store: KVStore) -> None:
    """Handle one client connection with line-delimited JSON requests."""
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
                    req = decode_request(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    conn.sendall(encode_response(False, error="invalid request"))
                    continue
                method = req.get("method")
                debug = req.get("debug_simulate_fail", False)
                try:
                    if method == "get":
                        key = req.get("key")
                        if key is None:
                            conn.sendall(encode_response(False, error="missing key"))
                            continue
                        value = store.get(key)
                        conn.sendall(encode_response(True, value=value))
                    elif method == "set":
                        key = req.get("key")
                        value = req.get("value")
                        if key is None:
                            conn.sendall(encode_response(False, error="missing key"))
                            continue
                        store.set(key, value, debug_simulate_fail=debug)
                        conn.sendall(encode_response(True))
                    elif method == "delete":
                        key = req.get("key")
                        if key is None:
                            conn.sendall(encode_response(False, error="missing key"))
                            continue
                        store.delete(key, debug_simulate_fail=debug)
                        conn.sendall(encode_response(True))
                    elif method == "bulk_set":
                        raw = req.get("items", [])
                        items = [tuple(pair) for pair in raw]
                        store.bulk_set(items, debug_simulate_fail=debug)
                        conn.sendall(encode_response(True))
                    elif method == "search":
                        query = req.get("query", "")
                        keys = getattr(store, "search_fulltext", lambda q: [])(query)
                        conn.sendall(encode_response(True, value=keys))
                    elif method == "search_similar":
                        query = req.get("query", "")
                        top_k = req.get("top_k", 10)
                        results = getattr(store, "search_similar", lambda q, top_k=10: [])(query, top_k)
                        conn.sendall(encode_response(True, value=results))
                    else:
                        conn.sendall(encode_response(False, error=f"unknown method: {method}"))
                except Exception as e:
                    conn.sendall(encode_response(False, error=str(e)))
    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        conn.close()


def run_server(
    host: str = "127.0.0.1",
    port: int = 9999,
    data_dir: str = "data",
    debug_random_fail_chance: float = 0.0,
    enable_indexes: bool = False,
) -> None:
    """Run the KV store TCP server."""
    store = KVStore(
        data_dir=data_dir,
        debug_random_fail_chance=debug_random_fail_chance,
        enable_indexes=enable_indexes,
    )
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(64)
    try:
        while True:
            conn, _ = server.accept()
            t = threading.Thread(target=handle_client, args=(conn, store))
            t.daemon = True
            t.start()
    finally:
        server.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--debug-fail-chance", type=float, default=0.0)
    parser.add_argument("--enable-indexes", action="store_true")
    args = parser.parse_args()
    run_server(
        host=args.host,
        port=args.port,
        data_dir=args.data_dir,
        debug_random_fail_chance=args.debug_fail_chance,
        enable_indexes=args.enable_indexes,
    )
