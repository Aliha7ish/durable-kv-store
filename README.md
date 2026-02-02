# Durable Key-Value Store

A TCP-based key-value store with persistence (WAL + snapshot), replication (primary/secondary and master-less), and optional indexes (full-text search, word-embedding similarity).

## Features

- **Set, Get, Delete, BulkSet** – core operations
- **Persistence** – Write-Ahead Log (WAL) + snapshot; survives restarts
- **TCP server** – line-delimited JSON protocol
- **Client** – `KVClient` with `Get(key)`, `Set(key, value)`, `Delete(key)`, `BulkSet([(key, value)])`
- **Debug** – `debug_simulate_fail` on Set/BulkSet to randomly skip snapshot (simulate fsync failure); WAL is always synchronous
- **Replication** – cluster of 3 nodes: 1 primary, 2 secondaries; election when primary goes down
- **Master-less** – all nodes accept reads/writes; replicate to all peers (last-writer-wins)
- **Indexes** – full-text search and word-embedding similarity on values (optional, `--enable-indexes`)

## Quick Start

```bash
# Install
pip install -r requirements.txt

# Run single-node server
python -m server --port 9999 --data-dir data

# In another terminal: use client
python -c "
from client import KVClient
c = KVClient(port=9999)
c.Set('hello', 'world')
print(c.Get('hello'))
"
```

## Tests

```bash
python -m pytest tests/ -v --timeout=60
```

Covers: Set then Get; Set then Delete then Get; Get without setting; Set twice same key; persistence after graceful exit; concurrent bulk set same keys; bulk + kill (all-or-nothing); debug random fail; cluster replication and election.

## Benchmarks

```bash
# Write throughput (pre-populated data sizes)
python -m benchmarks.bench_throughput

# Durability: writer + random kill; restart and check acknowledged keys
python -m benchmarks.bench_durability
```

## Cluster (Primary + Secondaries)

```bash
# Start 3 nodes (node 0 = primary)
# Use tests/test_cluster.py as reference or a launcher script that starts 3 processes with:
#   cluster_node --node-id 0 --kv-port P0 --repl-port R0 --secondary-repl-ports R1 R2 --data-dir d0 --other-kv-ports P1 P2
#   cluster_node --node-id 1 --kv-port P1 --repl-port R1 --secondary-repl-ports R0 R2 --data-dir d1 --other-kv-ports P0 P2
#   cluster_node --node-id 2 --kv-port P2 --repl-port R2 --secondary-repl-ports R0 R1 --data-dir d2 --other-kv-ports P0 P1
```

Client discovers primary via `ClusterKVClient(kv_ports=[P0, P1, P2])` and uses it for Get/Set/Delete/BulkSet.

## Master-less

```bash
# Start 3 nodes; each accepts writes and replicates to others
python -m masterless_node --node-id 0 --kv-port P0 --repl-port R0 --peer-repl-ports R1 R2 --data-dir d0
# ... same for node 1 and 2 with peer-repl-ports pointing to the other two
```

Use any node’s KV port for reads/writes.

## Indexes (Full-Text and Word Embedding)

```bash
python -m server --port 9999 --data-dir data --enable-indexes
```

Then:

```python
from client import KVClient
c = KVClient(port=9999)
c.Set("doc1", "hello world foo")
c.Set("doc2", "hello bar")
c.Search("hello")        # full-text: keys containing "hello"
c.SearchSimilar("foo", top_k=5)  # embedding similarity
```

## ACID and Durability

- WAL is written and fsync’d before responding; snapshot can be skipped with debug flag.
- BulkSet is one WAL entry; kill during write gives all-or-nothing after restart.
- For killing in tests/benchmarks use `process.kill()` (SIGKILL on Unix) or equivalent.

## License

MIT.
