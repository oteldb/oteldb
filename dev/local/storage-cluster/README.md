# Clustered storage-engine demo

A three-node **oteldb cluster on the embedded storage engine** — no ClickHouse and **no shared
object store**. Each node keeps its data on a **local file backend** and replicates writes to its
peers (RF=2) over an etcd-coordinated [rendezvous-hash](https://en.wikipedia.org/wiki/Rendezvous_hashing)
ring. This is the shared-nothing model: data is sharded and replicated across the nodes' own disks.

## Run

```bash
docker compose -f dev/local/storage-cluster/docker-compose.yml up --build
```

Open Grafana at <http://localhost:3000> (anonymous admin) — PromQL, LogQL, and TraceQL datasources
are pre-provisioned against `oteldb-1`.

## Topology

```
                 ┌──────── etcd ────────┐   (membership + ring state)
                 │          │           │
            ┌─ oteldb-1 ─ oteldb-2 ─ oteldb-3 ─┐   each: local file backend, RF=2 replication
            │     ▲           ▲                │
   client ──┘  OTLP        OTLP └── server     │
   (ingest @ oteldb-1)   (ingest @ oteldb-2)   │
                 │                             │
              Grafana ── queries oteldb-1 ─────┘  (fans out across the ring, merges replicas)
```

| Service    | Role | Host ports |
|------------|------|------------|
| `etcd`     | membership + ring coordination | — |
| `oteldb-1` | cluster node (file backend) | 9090, 3100, 3200, 4040, 4317 |
| `oteldb-2` | cluster node (file backend) | 9091 → its PromQL |
| `oteldb-3` | cluster node (file backend) | 9092 → its PromQL |
| `client`/`server` | OTLP generators (logs/metrics/traces) | — |
| `grafana`  | dashboards | 3000 |

## What it demonstrates

- **Ingest anywhere.** `client` pushes OTLP to `oteldb-1` and `server` pushes to `oteldb-2`; each
  node routes every write to the ring's owning replicas. Nothing is pinned to the node that received
  it.
- **Query anywhere.** Grafana queries only `oteldb-1`, yet sees data ingested at `oteldb-2` too,
  because a query fans out across the ring and merges replicas. Compare `oteldb-1` (host `:9090`),
  `oteldb-2` (`:9091`), and `oteldb-3` (`:9092`) — `up`-style queries return the same series.
- **Replication / failure tolerance.** With RF=2 every series lives on two of the three nodes. Stop
  one (`docker compose -f dev/local/storage-cluster/docker-compose.yml stop oteldb-2`) and queries
  still return its data from the replica; restart it and it rejoins the ring.

## Configuration

Every node runs `oteldb --embedded` with the shared [`oteldb.yml`](./oteldb.yml). The only thing that
differs between nodes is the container `hostname`: the cluster id and replication address default to
it, so `oteldb-1` advertises `oteldb-1:7946`, `oteldb-2` advertises `oteldb-2:7946`, and so on. The
cluster block:

```yaml
storage:
  backend: file
  dir: /data
  cluster:
    etcd: ["http://etcd:2379"]
    port: 7946   # replication server port; address is <hostname>:<port>
    rf: 2        # replicas per write
    root: /oteldb
```

To scale out, add another `oteldb-N` service (with its own `hostname` and data volume) pointing at
the same etcd — it joins the ring and takes ownership of a share of the data automatically.

## Automated test

The **Cluster E2E** CI job (`.github/workflows/cluster-e2e.yml`) starts this stack with the
[`docker-compose.ci.yml`](./docker-compose.ci.yml) overlay and runs
[`cmd/cluster-verify`](./cmd/cluster-verify), which pushes one metric, log, and trace via OTLP to one
node and asserts they are served by the PromQL/LogQL/TraceQL APIs of other nodes — exercising
cross-node routing and replication for every signal. Run it locally with:

```bash
docker compose -f docker-compose.yml -f docker-compose.ci.yml up -d --build oteldb-1 oteldb-2 oteldb-3
go run ./cmd/cluster-verify -otlp localhost:14317 -prometheus http://localhost:9092 \
  -loki http://localhost:3100 -tempo http://localhost:3200
```

