import { Fragment } from "react";
import { useGetStorage } from "../api/admin";
import { Card, Bar, Chip, KV, Mono, Spinner, ErrorBox } from "../components/ui";
import { fmtBytes, fmtNum, fmtTime } from "../lib/format";
import type { CacheStats, ClusterStats, TenantStats } from "../api/model";

function Caches({ caches }: { caches: CacheStats }) {
  const dc = caches.decode_cache;
  const total = dc.hits + dc.misses;
  const rate = total ? dc.hits / total : 0;
  return (
    <Card title="Caches" sub="decode">
      <Bar label="decode hit rate" value={(rate * 100).toFixed(1) + "%"} ratio={rate} />
      <KV
        rows={[
          ["cached bytes", <Mono>{fmtBytes(dc.bytes)}</Mono>],
          ["cached blocks", <Mono>{fmtNum(dc.items)}</Mono>],
          ["hits / misses", <Mono>{fmtNum(dc.hits) + " / " + fmtNum(dc.misses)}</Mono>],
        ]}
      />
    </Card>
  );
}

function Cluster({ cluster }: { cluster: ClusterStats }) {
  return (
    <Card title="Cluster">
      <KV rows={[["owned shards", <Mono>{cluster.owned.length}</Mono>]]} />
      <div className="scroll" style={{ marginTop: 10 }}>
        <table>
          <thead>
            <tr>
              <th>member</th>
              <th>zone</th>
              <th>addr</th>
            </tr>
          </thead>
          <tbody>
            {cluster.members.map((m) => (
              <tr key={m.id}>
                <td>
                  {m.id}
                  {m.id === cluster.self ? (
                    <>
                      {" "}
                      <Chip on>self</Chip>
                    </>
                  ) : null}
                </td>
                <td>{m.zone || "—"}</td>
                <td>{m.addr || "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

function Tenants({ tenants }: { tenants: TenantStats[] }) {
  return (
    <Card title="Tenants & signals" wide scroll style={{ marginTop: 14 }}>
      <table>
        <thead>
          <tr>
            <th>tenant / signal</th>
            <th>head items</th>
            <th>head bytes</th>
            <th>series</th>
            <th>parts</th>
            <th>WAL</th>
            <th>WAL bytes</th>
            <th>min time</th>
            <th>max time</th>
          </tr>
        </thead>
        <tbody>
          {tenants.map((t) => {
            const a = t.admission;
            const rejected =
              a.rejected_ooo + a.rejected_rate + a.rejected_cardinality + a.rejected_in_flight;
            return (
              <Fragment key={t.tenant}>
                <tr style={{ background: "var(--surface-2)" }}>
                  <td>{t.tenant}</td>
                  <td colSpan={2}>
                    accepted {fmtNum(a.accepted)}
                    {rejected ? (
                      <span style={{ color: "var(--red)" }}> · rejected {fmtNum(rejected)}</span>
                    ) : null}
                  </td>
                  <td>{fmtNum(t.total_series)}</td>
                  <td>{fmtNum(t.total_parts)}</td>
                  <td colSpan={4} />
                </tr>
                {t.signals.map((s) => (
                  <tr key={t.tenant + "/" + s.signal}>
                    <td style={{ paddingLeft: 22, color: "var(--muted)" }}>{s.signal}</td>
                    <td>{fmtNum(s.head_items)}</td>
                    <td>{fmtBytes(s.head_bytes)}</td>
                    <td>{fmtNum(s.series)}</td>
                    <td>
                      {fmtNum(s.parts)}
                      {s.merge_running ? (
                        <>
                          {" "}
                          <Chip on>merging</Chip>
                        </>
                      ) : null}
                    </td>
                    <td>{s.wal ? fmtNum(s.wal_segments) + " seg" : "—"}</td>
                    <td>{s.wal ? fmtBytes(s.wal_bytes) : "—"}</td>
                    <td>{fmtTime(s.min_time)}</td>
                    <td>{fmtTime(s.max_time)}</td>
                  </tr>
                ))}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </Card>
  );
}

export function Storage() {
  const { data, isLoading, error } = useGetStorage({ query: { refetchInterval: 8_000 } });

  if (isLoading) return <Spinner />;
  if (error) return <ErrorBox error={error} />;
  if (!data) return null;

  const eng = data.engine;
  const ch = data.clickhouse;

  return (
    <>
      <div className="section-title">Embedded storage engine</div>
      {!eng ? (
        <div className="banner">
          <span className="i">i</span>
          <div>The embedded oteldb/storage engine is not active on this instance.</div>
        </div>
      ) : (
        <>
          <div className="grid">
            <Caches caches={eng.caches} />
            {eng.cluster ? <Cluster cluster={eng.cluster} /> : null}
          </div>
          {eng.tenants.length ? <Tenants tenants={eng.tenants} /> : null}
        </>
      )}

      {ch ? (
        <>
          <div className="section-title">ClickHouse (deprecated)</div>
          <Card title="Tables" wide scroll>
            {!ch.tables.length ? (
              <p className="empty">No tables.</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>table</th>
                    <th>rows</th>
                    <th>on disk</th>
                    <th>uncompressed</th>
                    <th>parts</th>
                    <th>min time</th>
                    <th>max time</th>
                  </tr>
                </thead>
                <tbody>
                  {ch.tables.map((t) => (
                    <tr key={t.database + "." + t.table}>
                      <td>{t.table}</td>
                      <td>{fmtNum(t.rows)}</td>
                      <td>{fmtBytes(t.bytes_on_disk)}</td>
                      <td>{fmtBytes(t.data_uncompressed_bytes)}</td>
                      <td>{fmtNum(t.parts)}</td>
                      <td>{fmtTime(t.min_time)}</td>
                      <td>{fmtTime(t.max_time)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </Card>
        </>
      ) : null}
    </>
  );
}
