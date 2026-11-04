import { Button, Flex, Table, Text } from "@gravity-ui/uikit";
import type { TableColumnConfig, TableDataItem } from "@gravity-ui/uikit";
import { useState } from "react";
import type {
  ClusterStats,
  EngineSignalStats,
  StorageNodeReport,
  StorageStats,
} from "../api/model";
import { fmtNum, fmtTime } from "../lib/format";
import { useSelectedNode } from "../lib/node";
import { Chip, ErrCount, head, Panel, Rule } from "./ui";

const UNKNOWN = "unknown";

function state(value: boolean | undefined, yes: string, no: string) {
  return value == null ? UNKNOWN : value ? yes : no;
}

function nanos(value: number | undefined) {
  return value == null ? "—" : fmtTime(new Date(value / 1e6).toISOString());
}

function readBoundary(signal: EngineSignalStats) {
  if (signal.index_fenced) return "index unavailable";
  if (signal.has_read_gap == null) return UNKNOWN;
  if (!signal.has_read_gap) return "no read gap";
  if (signal.read_gap_after_unix_nano == null) return UNKNOWN;
  if (signal.read_gap_after_unix_nano === -(2 ** 63)) return "no complete interval";
  return `incomplete after ${nanos(signal.read_gap_after_unix_nano)}`;
}

function gapCount(report: StorageNodeReport) {
  if (!report.engine) return "—";
  const signals = report.engine.tenants.flatMap((t) => t.signals);
  if (!signals.length && report.engine.cluster?.private_backend == null) return UNKNOWN;
  const count = signals.filter((s) => s.has_read_gap).length;
  return signals.some((s) => s.has_read_gap == null)
    ? `${count} · coverage unknown`
    : fmtNum(count);
}

function LimitedTable<T extends TableDataItem>({
  data,
  columns,
  rowId,
}: {
  data: T[];
  columns: TableColumnConfig<T>[];
  rowId: (row: T) => string;
}) {
  const [limit, setLimit] = useState(100);
  return (
    <Flex direction="column" gap={3}>
      <Table data={data.slice(0, limit)} columns={columns} getRowId={rowId} />
      {data.length > limit ? (
        <Button view="outlined" onClick={() => setLimit((n) => n + 100)}>
          Show more ({fmtNum(data.length - limit)} remaining)
        </Button>
      ) : null}
    </Flex>
  );
}

type ShardIssue = { id: string; node: string; tenant: string; signal: EngineSignalStats };

const ISSUE_COLUMNS: TableColumnConfig<ShardIssue>[] = [
  { id: "node", name: head("reporting node"), primary: true },
  { id: "tenant", name: head("tenant / shard") },
  { id: "signal", name: head("signal"), template: (r) => r.signal.signal },
  {
    id: "read_gap",
    name: head("read coverage"),
    template: (r) => (
      <ErrCount bad={r.signal.has_read_gap === true}>{readBoundary(r.signal)}</ErrCount>
    ),
  },
  {
    id: "state",
    name: head("local state"),
    template: ({ signal: s }) => (
      <Flex gap={1} wrap>
        {s.index_fenced ? <Chip theme="danger">index fenced</Chip> : null}
        {s.tenure_unestablished ? <Chip theme="warning">claim not established</Chip> : null}
        {s.out_of_space ? <Chip theme="danger">out of space</Chip> : null}
        {(s.wanted_parts ?? 0) > 0 ? <Chip theme="warning">repair pending</Chip> : null}
        {(s.holes ?? 0) > 0 ? <Chip theme="danger">missing data</Chip> : null}
      </Flex>
    ),
  },
  { id: "wanted", name: head("wanted parts"), template: (r) => fmtNum(r.signal.wanted_parts) },
  { id: "holes", name: head("current holes"), template: (r) => fmtNum(r.signal.holes) },
  { id: "lost", name: head("historical losses"), template: (r) => fmtNum(r.signal.lost_parts) },
  { id: "error", name: head("index error"), template: (r) => r.signal.index_load_error || "—" },
];

function shardIssues(reports: StorageNodeReport[]): ShardIssue[] {
  return reports.flatMap((r) =>
    (r.engine?.tenants ?? []).flatMap((t) =>
      t.signals
        .filter(
          (s) =>
            s.has_read_gap ||
            s.index_fenced ||
            s.tenure_unestablished ||
            s.out_of_space ||
            (s.wanted_parts ?? 0) > 0 ||
            (s.holes ?? 0) > 0 ||
            (s.lost_parts ?? 0) > 0,
        )
        .map((signal) => ({
          id: JSON.stringify([r.node, t.tenant, signal.signal]),
          node: r.node,
          tenant: t.tenant,
          signal,
        })),
    ),
  );
}

type PlacementRow = { node: string; shard: string; owners: string[] };
const PLACEMENT_COLUMNS: TableColumnConfig<PlacementRow>[] = [
  { id: "node", name: head("reporting node"), primary: true },
  { id: "shard", name: head("shard") },
  {
    id: "owners",
    name: head("routing owners · slot order"),
    template: (r) => r.owners.join(" → ") || "no owners reported",
  },
];

type MoveRow = { node: string; shard: string; added: string[]; removed: string[] };
const MOVE_COLUMNS: TableColumnConfig<MoveRow>[] = [
  { id: "node", name: head("reporting node"), primary: true },
  { id: "shard", name: head("shard") },
  { id: "added", name: head("added owners"), template: (r) => r.added.join(", ") || "—" },
  { id: "removed", name: head("removed owners"), template: (r) => r.removed.join(", ") || "—" },
];

function NodeDetails({ cluster }: { cluster: ClusterStats }) {
  return (
    <Panel title="Reported ring view" sub={cluster.self || "legacy aggregate"} scroll>
      <Flex direction="column" gap={4}>
        <Text color="secondary">
          Compaction claims: {cluster.owned.join(", ") || "none"}. These claims are separate from
          replica routing owner sets.
        </Text>
        <Table
          data={cluster.members}
          columns={[
            { id: "id", name: head("member"), primary: true },
            { id: "zone", name: head("zone"), template: (m) => m.zone || "—" },
            { id: "addr", name: head("address"), template: (m) => m.addr || "—" },
          ]}
          getRowId="id"
        />
      </Flex>
    </Panel>
  );
}

export function StorageCluster({ data }: { data: StorageStats }) {
  const { setNode } = useSelectedNode();
  const aggregate = data.node_reports != null;
  if (!aggregate && !data.engine?.cluster) return null;
  const legacyAggregate = !aggregate && data.engine?.cluster?.self === "";
  const reports: StorageNodeReport[] = data.node_reports ?? [
    {
      node: data.engine?.cluster?.self || "aggregate (legacy)",
      status: "ok",
      engine: data.engine,
    },
  ];
  const issues = shardIssues(reports);
  const placements = reports.flatMap((r) =>
    (r.engine?.cluster?.placements ?? []).map((p) => ({ ...p, node: r.node })),
  );
  const moves = reports.flatMap((r) =>
    (r.engine?.cluster?.last_rebalance ?? []).map((m) => ({ ...m, node: r.node })),
  );
  const columns: TableColumnConfig<StorageNodeReport>[] = [
    {
      id: "node",
      name: head("node"),
      primary: true,
      template: (r) =>
        aggregate ? (
          <Button
            view="flat"
            aria-label={`Inspect ${r.node}`}
            disabled={r.status !== "ok"}
            onClick={() => setNode(r.node)}
          >
            {r.node}
          </Button>
        ) : (
          r.node
        ),
    },
    {
      id: "status",
      name: head("observation"),
      template: (r) => <ErrCount bad={r.status !== "ok"}>{r.status}</ErrCount>,
    },
    {
      id: "backend",
      name: head("backend mode"),
      template: (r) => state(r.engine?.cluster?.private_backend, "private", "shared"),
    },
    {
      id: "fenced",
      name: head("write fence"),
      template: (r) => (
        <ErrCount bad={r.engine?.cluster?.fenced === true}>
          {state(r.engine?.cluster?.fenced, "fenced", "not fenced")}
        </ErrCount>
      ),
    },
    {
      id: "membership",
      name: head("membership"),
      template: (r) => (
        <ErrCount bad={r.engine?.cluster?.self_absent === true}>
          {state(r.engine?.cluster?.self_absent, "self absent", "present")}
        </ErrCount>
      ),
    },
    { id: "rejoins", name: head("rejoins"), template: (r) => fmtNum(r.engine?.cluster?.rejoins) },
    {
      id: "fence_deadline",
      name: head("fence deadline"),
      template: (r) => {
        const n = r.engine?.cluster?.fence_deadline_unix_nano;
        return n == null ? UNKNOWN : n === 0 ? "no lease" : nanos(n);
      },
    },
    { id: "gaps", name: head("read gaps"), template: gapCount },
    { id: "error", name: head("request error"), template: (r) => r.error || "—" },
  ];
  return (
    <Flex direction="column" gap={4}>
      <Rule>Cluster state</Rule>
      <Text color="secondary">
        Each row is a node observation, not a globally consistent snapshot. Unreachable nodes and
        fields absent from older nodes remain unknown. Select a node to inspect its local ring view.
      </Text>
      {legacyAggregate ? (
        <Text color="secondary">
          This older aggregator supplies totals without per-node observations. Select a node for its
          own report; the legacy membership list and claim union are retained below.
        </Text>
      ) : null}
      <Panel title="Node observations" scroll>
        <Table data={reports} columns={columns} getRowId="node" />
      </Panel>
      {!aggregate && data.engine?.cluster ? <NodeDetails cluster={data.engine.cluster} /> : null}
      <Panel
        title="Read coverage & storage faults"
        sub={`${issues.length} reported shard states`}
        scroll
      >
        <Flex direction="column" gap={3}>
          {issues.length ? (
            <LimitedTable data={issues} columns={ISSUE_COLUMNS} rowId={(r) => r.id} />
          ) : (
            <Text>No read gaps or storage faults were reported by the responding nodes.</Text>
          )}
          <Text color="secondary">
            Read gaps trigger failover; they do not prove data loss. Current holes identify missing
            data, while historical losses can remain nonzero after recovery. Coverage is unavailable
            for unreachable nodes and signals without the new fields.
          </Text>
        </Flex>
      </Panel>
      <Panel title="Shard placement" sub="node-reported owner sets" scroll>
        <Flex direction="column" gap={3}>
          {placements.length ? (
            <LimitedTable
              data={placements}
              columns={PLACEMENT_COLUMNS}
              rowId={(r) => JSON.stringify([r.node, r.shard])}
            />
          ) : (
            <Text>
              No owner sets reported; inspect a current storage node for placement details.
            </Text>
          )}
          <Text color="secondary">
            Only locally known engine, claim and last-rebalance shard keys are included. Owner sets
            are kept per reporting node so disagreements remain visible; this is not a complete
            shard inventory.
          </Text>
        </Flex>
      </Panel>
      <Panel title="Last enacted rebalance" sub="owner-set changes" scroll>
        {moves.length ? (
          <LimitedTable
            data={moves}
            columns={MOVE_COLUMNS}
            rowId={(r) => JSON.stringify([r.node, r.shard])}
          />
        ) : (
          <Text color="secondary">
            No rebalance moves reported; older nodes may omit this data.
          </Text>
        )}
      </Panel>
    </Flex>
  );
}
