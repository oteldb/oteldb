# Clustered storage-engine demo

A three-node **oteldb cluster on the embedded storage engine** — no ClickHouse. Nodes coordinate
through an etcd-backed [rendezvous-hash](https://en.wikipedia.org/wiki/Rendezvous_hashing) ring.

Both storage models are runnable here:

- **shared-nothing** (default) — each node keeps its data on a **local file backend** and replicates
  writes to its peers (RF=2). Data is sharded and replicated across the nodes' own disks.
- **shared-store** ([below](#shared-store-variant)) — every node reads and writes **one object
  store**, so flushed parts need no mirroring.

Either storage model can also be run **split** ([below](#split-process-variant)): ingest and query
move out of the nodes into `odbingest` and `odbselect`, their own stateless processes.

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
    private_backend: true   # each node's disk is its own; parts replicate node-to-node
```

`private_backend` picks the storage model, and it is not inferable from the backend type — an S3
bucket shared by every node and a per-node local disk are both legal backends. Here each node has
its own `/data` volume that peers cannot read, so it must be `true`: flushed parts are mirrored over
the cluster's parts endpoints (`partsync`) instead of being exchanged through a shared store. Left
at the default `false` writes still reach the replicas, but anything that reads another node's
flushed parts breaks — rebalance handoffs, a newly-promoted owner, and backfill before compaction
would all have no source. A deployment where every node points at the same object store wants
`false`.

To scale out, add another `oteldb-N` service (with its own `hostname` and data volume) pointing at
the same etcd — it joins the ring and takes ownership of a share of the data automatically.

## Shared-store variant

The default stack above is **shared-nothing**: each node owns its disk and mirrors flushed parts to
its peers. The other deployment model is **shared-store** — every node reads and writes one object
store under one prefix, so a flushed part is already visible to every peer and there is nothing to
mirror.

```bash
docker compose -f dev/local/storage-cluster/docker-compose.yml \
               -f dev/local/storage-cluster/docker-compose.shared-store.yml \
               --profile minio up --build
```

Same three nodes, same etcd, same ring; [`oteldb-shared.yml`](./oteldb-shared.yml) replaces the file
backend with `backend: s3` and flips `private_backend` to `false`. The WAL stays on each node's own
volume — it holds head data that has not reached the bucket yet, so it has to survive a restart of
*that* node. Browse what the cluster writes at <http://localhost:9001> (MinIO console,
`oteldb`/`oteldbsecret`).

### What this exercises that the default cannot

Several writers committing **one bucket index**. Each node commits through
`backend.CompareAndSwap`, which on S3 is a conditional PUT (`If-Match`, or `If-None-Match: *` for
the first write) — the *store* evaluates the condition, so two nodes committing at once cannot
overwrite each other's parts. Nothing but a real object store evaluates that the way a deployment
would, which is the whole reason this variant exists as a runnable stack rather than a unit test.

### Choosing the object store

| profile | store | why |
|---|---|---|
| `minio` (default) | MinIO | An independent, widely deployed implementation. A green run is evidence about oteldb. |
| `fs` | [go-faster/fs](https://github.com/go-faster/fs) in single-node filesystem mode | A sibling project. Lighter, but a green run says the two agree — not the same claim. |

Pick deliberately. If the conditional-PUT path breaks against a store you also maintain, you cannot
tell from the failure which side is wrong. Use `--profile fs` when you want to exercise go-faster/fs
against a real workload, and `--profile minio` when you want to make a claim about oteldb.

Neither is a substitute for running against the store you deploy on: `If-Match` on PUT is not
universal among S3-compatible services, and where it is missing `CompareAndSwap` has no ground to
stand on.

## Split-process variant

By default every node runs `--embedded`, so one process both stores and serves. In production the
two halves scale independently, and they are separate binaries:

- **`odbingest`** — takes OTLP and Prometheus remote write, and routes each shard to its ring primary.
- **`odbselect`** — answers PromQL/LogQL/TraceQL/ProfileQL by fanning out across the ring and
  merging replicas.

Both are stateless: they follow membership read-only, never join the ring, and hold no data. The
three nodes are unchanged — they simply stop being the only front door.

```bash
docker compose -f dev/local/storage-cluster/docker-compose.yml \
               -f dev/local/storage-cluster/docker-compose.split.yml up --build
```

The demo `server` ingests through `odbingest` while `client` keeps ingesting at `oteldb-1`, so both
write paths carry traffic at once, and Grafana gets a second set of datasources ("PromQL (split)"
and friends) pointing at `odbselect`. The two paths serve the same cluster, so the same data should
appear through either — which is the point of having both in one dashboard.

| | embedded | split |
|---|---|---|
| PromQL | <http://localhost:9090> | <http://localhost:19090> |
| LogQL | <http://localhost:3100> | <http://localhost:13100> |
| TraceQL | <http://localhost:3200> | <http://localhost:13200> |
| ProfileQL | <http://localhost:4040> | <http://localhost:14040> |
| OTLP gRPC | <localhost:4317> | <localhost:24317> |

This composes with the shared-store overlay: add both files to put split processes in front of
nodes that share one bucket.

## Automated test

The **Cluster E2E** CI job (`.github/workflows/cluster-e2e.yml`) starts this stack with the
[`docker-compose.ci.yml`](./docker-compose.ci.yml) overlay and runs
[`cmd/cluster-verify`](./cmd/cluster-verify), which pushes one metric, log, and trace via OTLP to one
node and asserts they are served by the PromQL/LogQL/TraceQL APIs of other nodes — exercising
cross-node routing and replication for every signal.

It runs once per model (`shared-nothing`, `shared-store`, `split`), because they fail differently:
the first two change where parts live, while `split` changes which process routes and fans out.
Run it locally with:

```bash
docker compose -f docker-compose.yml -f docker-compose.ci.yml up -d --build oteldb-1 oteldb-2 oteldb-3
go run ./cmd/cluster-verify -otlp localhost:14317 -prometheus http://localhost:9092 \
  -loki http://localhost:3100 -tempo http://localhost:3200
```

Or against the split processes:

```bash
docker compose -f docker-compose.yml -f docker-compose.ci.yml \
               -f docker-compose.split.yml -f docker-compose.split.ci.yml \
               up -d --build oteldb-1 oteldb-2 oteldb-3 odbingest odbselect
go run ./cmd/cluster-verify -otlp localhost:24317 -prometheus http://localhost:19090 \
  -loki http://localhost:13100 -tempo http://localhost:13200
```

