import { Alert, Col, Flex, Row, Table, Text } from "@gravity-ui/uikit";
import type { ColProps, TableColumnConfig } from "@gravity-ui/uikit";
import { useGetEfficiency, useGetStorage } from "../api/admin";
import {
  Chip,
  ErrCount,
  ErrorAlert,
  head,
  KV,
  Loading,
  Mono,
  Panel,
  Rule,
  UsageBar,
} from "../components/ui";
import { fmtBytes, fmtNum, fmtTime } from "../lib/format";
import type {
  CacheStats,
  ClusterMember,
  ClusterStats,
  ECStats,
  EngineSignalStats,
  MaintenanceStats,
  PartSyncStats,
  SignalEfficiency,
  TableStats,
  TenantEfficiency,
  TenantStats,
} from "../api/model";

const COL: ColProps["size"] = [12, { m: 6, xl: 4 }];

function Caches({ caches }: { caches: CacheStats }) {
  const dc = caches.decode_cache;
  const total = dc.hits + dc.misses;
  const rate = total ? dc.hits / total : 0;
  return (
    <Panel title="Caches" sub="decode">
      <Flex direction="column" gap={4}>
        <UsageBar
          label="decode hit rate"
          value={`${(rate * 100).toFixed(1)}%`}
          ratio={rate}
        />
        <KV
          rows={[
            ["cached bytes", <Mono>{fmtBytes(dc.bytes)}</Mono>],
            ["cached blocks", <Mono>{fmtNum(dc.items)}</Mono>],
            ["hits / misses", <Mono>{`${fmtNum(dc.hits)} / ${fmtNum(dc.misses)}`}</Mono>],
          ]}
        />
      </Flex>
    </Panel>
  );
}

function MaintenanceLoop({ m }: { m: MaintenanceStats }) {
  return (
    <Panel title="Maintenance loop" sub="flush · merge · retention">
      <KV
        rows={[
          ["cycles", <Mono>{fmtNum(m.cycles)}</Mono>],
          ["last cycle start", <Mono>{fmtTime(m.last_cycle_start)}</Mono>],
          [
            "last cycle duration",
            <Mono>{m.cycles ? `${m.last_cycle_duration_seconds.toFixed(2)} s` : "—"}</Mono>,
          ],
          ["last cycle tasks", <Mono>{fmtNum(m.last_cycle_tasks)}</Mono>],
        ]}
      />
    </Panel>
  );
}

function PartSync({ ps }: { ps: PartSyncStats }) {
  return (
    <Panel title="Part mirroring" sub="shared-nothing replication">
      <KV
        rows={[
          ["passes", <Mono>{fmtNum(ps.passes)}</Mono>],
          ["mirrored", <Mono>{fmtNum(ps.mirrored)}</Mono>],
          ["copied", <Mono>{`${fmtNum(ps.copied)} obj · ${fmtBytes(ps.copied_bytes)}`}</Mono>],
          ["pruned", <Mono>{fmtNum(ps.pruned)}</Mono>],
          ["errors", <ErrCount bad={ps.errors > 0}>{fmtNum(ps.errors)}</ErrCount>],
          ["last sync", <Mono>{fmtTime(ps.last_sync)}</Mono>],
        ]}
      />
    </Panel>
  );
}

function ErasureCoding({ ec }: { ec: ECStats }) {
  const errs = ec.convert_errors + ec.repair_errors + ec.reconstruct_errors;
  const count = (ok: number, bad: number) => (
    <Flex alignItems="baseline" gap={1}>
      <Mono>{fmtNum(ok)}</Mono>
      {bad ? <ErrCount bad>{`· ${fmtNum(bad)} err`}</ErrCount> : null}
    </Flex>
  );
  return (
    <Panel title="Erasure coding" sub={errs ? "errors present" : "healthy"}>
      <KV
        rows={[
          ["converted parts", count(ec.converted, ec.convert_errors)],
          ["repaired slots", count(ec.repaired_slots, ec.repair_errors)],
          ["pruned staged parts", <Mono>{fmtNum(ec.pruned_staged_parts)}</Mono>],
          ["read reconstructs", count(ec.reconstructs, ec.reconstruct_errors)],
        ]}
      />
    </Panel>
  );
}

function Cluster({ cluster }: { cluster: ClusterStats }) {
  const columns: TableColumnConfig<ClusterMember>[] = [
    {
      id: "id",
      name: head("member"),
      template: (m) => (
        <Flex alignItems="center" gap={2}>
          <Mono>{m.id}</Mono>
          {m.id === cluster.self ? <Chip on>self</Chip> : null}
        </Flex>
      ),
    },
    { id: "zone", name: head("zone"), template: (m) => m.zone || "—" },
    { id: "addr", name: head("addr"), template: (m) => <Mono>{m.addr || "—"}</Mono> },
  ];
  return (
    <Panel title="Cluster" sub={`${cluster.owned.length} owned shards`} scroll>
      <Table data={cluster.members} columns={columns} getRowId="id" />
    </Panel>
  );
}

const SIGNAL_COLUMNS: TableColumnConfig<EngineSignalStats>[] = [
  { id: "signal", name: head("signal"), primary: true },
  { id: "head_items", name: head("head items"), align: "end", template: (s) => fmtNum(s.head_items) },
  { id: "head_bytes", name: head("head bytes"), align: "end", template: (s) => fmtBytes(s.head_bytes) },
  { id: "series", name: head("series"), align: "end", template: (s) => fmtNum(s.series) },
  {
    id: "parts",
    name: head("parts"),
    align: "end",
    template: (s) => (
      <Flex alignItems="center" gap={2} justifyContent="flex-end">
        {fmtNum(s.parts)}
        {s.merge_running ? <Chip on>merging</Chip> : null}
      </Flex>
    ),
  },
  { id: "sealed_parts", name: head("sealed"), align: "end", template: (s) => fmtNum(s.sealed_parts) },
  {
    id: "merge",
    name: head("merge backlog"),
    align: "end",
    // Backlog with no candidates is the stuck state: parts a merge may still take, none of which
    // any run qualifies to select, so every cycle is a no-op and the part count never falls.
    // storage-compact on the Maintenance page is what breaks it.
    template: (s) => (
      <Flex alignItems="center" gap={2} justifyContent="flex-end">
        <Mono>
          {fmtNum(s.merge_backlog)} / {fmtNum(s.merge_candidates)}
        </Mono>
        {s.merge_backlog > 0 && s.merge_candidates === 0 && !s.merge_running ? (
          <Chip theme="warning">stuck</Chip>
        ) : null}
      </Flex>
    ),
  },
  {
    id: "merge_cap_bytes",
    name: head("merge cap"),
    align: "end",
    template: (s) => (s.merge_cap_bytes > 0 ? fmtBytes(s.merge_cap_bytes) : "—"),
  },
  {
    id: "wal",
    name: head("WAL"),
    align: "end",
    template: (s) => (s.wal ? `${fmtNum(s.wal_segments)} seg` : "—"),
  },
  {
    id: "wal_bytes",
    name: head("WAL bytes"),
    align: "end",
    template: (s) => (s.wal ? fmtBytes(s.wal_bytes) : "—"),
  },
  { id: "min_time", name: head("min time"), template: (s) => fmtTime(s.min_time) },
  { id: "max_time", name: head("max time"), template: (s) => fmtTime(s.max_time) },
];

function Tenant({ t }: { t: TenantStats }) {
  const a = t.admission;
  const rejected =
    a.rejected_ooo + a.rejected_rate + a.rejected_cardinality + a.rejected_in_flight;
  return (
    <Panel
      title={t.tenant}
      sub={`${fmtNum(t.total_series)} series · ${fmtNum(t.total_parts)} parts`}
      actions={
        <Flex alignItems="center" gap={2}>
          <Text variant="body-1" color="secondary">
            accepted <Mono>{fmtNum(a.accepted)}</Mono>
          </Text>
          {rejected ? (
            <ErrCount bad>{`rejected ${fmtNum(rejected)}`}</ErrCount>
          ) : null}
        </Flex>
      }
      scroll
    >
      <Table data={t.signals} columns={SIGNAL_COLUMNS} getRowId="signal" />
    </Panel>
  );
}

type EfficiencyRow = SignalEfficiency & { tenant: string };

const EFFICIENCY_COLUMNS: TableColumnConfig<EfficiencyRow>[] = [
  { id: "tenant", name: head("tenant"), primary: true },
  { id: "signal", name: head("signal") },
  { id: "series", name: head("series"), align: "end", template: (s) => fmtNum(s.series) },
  { id: "parts", name: head("parts"), align: "end", template: (s) => fmtNum(s.parts) },
  { id: "points", name: head("points"), align: "end", template: (s) => fmtNum(s.points) },
  { id: "stored_bytes", name: head("stored"), align: "end", template: (s) => fmtBytes(s.stored_bytes) },
  {
    id: "bytes_per_point",
    name: head("bytes / point"),
    align: "end",
    template: (s) => (s.points ? s.bytes_per_point.toFixed(1) : "—"),
  },
  {
    id: "logical_bytes",
    name: head("logical"),
    align: "end",
    template: (s) => (s.logical_bytes != null ? fmtBytes(s.logical_bytes) : "—"),
  },
  {
    id: "compression_ratio",
    name: head("compression"),
    align: "end",
    template: (s) => (s.compression_ratio != null ? `${s.compression_ratio.toFixed(1)}×` : "—"),
  },
];

function flattenEfficiency(tenants: TenantEfficiency[]): EfficiencyRow[] {
  return tenants.flatMap((t) => t.signals.map((s) => ({ ...s, tenant: t.tenant })));
}

function Efficiency() {
  // Efficiency stats do backend I/O on the server — poll at a slower cadence.
  const { data, isLoading, error } = useGetEfficiency({ query: { refetchInterval: 30_000 } });

  if (isLoading) return <Loading />;
  if (error) return <ErrorAlert error={error} what="efficiency stats" />;
  if (!data || !data.tenants.length) return null;

  const rows = flattenEfficiency(data.tenants);

  return (
    <Panel title="Capacity & efficiency" sub="stored bytes · compression" scroll>
      <Table
        data={rows}
        columns={EFFICIENCY_COLUMNS}
        getRowId={(row) => `${row.tenant}/${row.signal}`}
      />
    </Panel>
  );
}

const CH_COLUMNS: TableColumnConfig<TableStats>[] = [
  { id: "table", name: head("table"), primary: true },
  { id: "rows", name: head("rows"), align: "end", template: (t) => fmtNum(t.rows) },
  { id: "bytes_on_disk", name: head("on disk"), align: "end", template: (t) => fmtBytes(t.bytes_on_disk) },
  {
    id: "data_uncompressed_bytes",
    name: head("uncompressed"),
    align: "end",
    template: (t) => fmtBytes(t.data_uncompressed_bytes),
  },
  { id: "parts", name: head("parts"), align: "end", template: (t) => fmtNum(t.parts) },
  { id: "min_time", name: head("min time"), template: (t) => fmtTime(t.min_time) },
  { id: "max_time", name: head("max time"), template: (t) => fmtTime(t.max_time) },
];

export function Storage() {
  const { data, isLoading, error } = useGetStorage({ query: { refetchInterval: 8_000 } });

  if (isLoading) return <Loading />;
  if (error) return <ErrorAlert error={error} what="storage stats" />;
  if (!data) return null;

  const eng = data.engine;
  const ch = data.clickhouse;

  return (
    <Flex direction="column" gap={5}>
      <Flex direction="column" gap={3}>
        <Rule>Embedded storage engine</Rule>
        {!eng ? (
          <Alert
            theme="info"
            view="outlined"
            title="The embedded engine is not active"
            message="Start oteldb with --embedded to serve signals from it."
          />
        ) : (
          <Row space="4" spaceRow="4">
            <Col size={COL}>
              <Caches caches={eng.caches} />
            </Col>
            <Col size={COL}>
              <MaintenanceLoop m={eng.maintenance} />
            </Col>
            {eng.cluster ? (
              <Col size={COL}>
                <Cluster cluster={eng.cluster} />
              </Col>
            ) : null}
            {eng.cluster?.part_sync ? (
              <Col size={COL}>
                <PartSync ps={eng.cluster.part_sync} />
              </Col>
            ) : null}
            {eng.cluster?.ec ? (
              <Col size={COL}>
                <ErasureCoding ec={eng.cluster.ec} />
              </Col>
            ) : null}
          </Row>
        )}
      </Flex>

      {eng?.tenants.length ? (
        <Flex direction="column" gap={3}>
          <Rule>Tenants &amp; signals</Rule>
          {eng.tenants.map((t) => (
            <Tenant key={t.tenant} t={t} />
          ))}
        </Flex>
      ) : null}

      {eng ? <Efficiency /> : null}

      {ch ? (
        <Flex direction="column" gap={3}>
          <Rule>ClickHouse (deprecated)</Rule>
          <Panel title="Tables" scroll>
            <Table
              data={ch.tables}
              columns={CH_COLUMNS}
              emptyMessage="No tables."
              getRowId={(row) => `${row.database}.${row.table}`}
            />
          </Panel>
        </Flex>
      ) : null}
    </Flex>
  );
}
