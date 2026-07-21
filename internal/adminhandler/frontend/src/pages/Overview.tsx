import { useGetInfo, useGetRuntime, useGetHealth } from "../api/admin";
import { Card, Bar, Pill, Chip, KV, Mono, Spinner, ErrorBox } from "../components/ui";
import { fmtBytes, fmtNum, fmtTime } from "../lib/format";

export function Overview() {
  const info = useGetInfo({ query: { refetchInterval: 10_000 } });
  const runtime = useGetRuntime({ query: { refetchInterval: 5_000 } });
  const health = useGetHealth({ query: { refetchInterval: 10_000 } });

  return (
    <>
      <div className="section-title">Overview</div>
      <div className="grid">
        <Card title="Instance">
          {info.isLoading ? (
            <Spinner />
          ) : info.error ? (
            <ErrorBox error={info.error} />
          ) : info.data ? (
            <KV
              rows={[
                [
                  "storage",
                  <Chip on>
                    {info.data.storage_enabled
                      ? "storage:" + (info.data.storage_backend || "?")
                      : info.data.clickhouse_enabled
                        ? "clickhouse"
                        : "—"}
                  </Chip>,
                ],
                ["platform", info.data.os + "/" + info.data.arch],
                ["commit", <Mono>{(info.data.commit || "—").slice(0, 12)}</Mono>],
                ["started", fmtTime(info.data.start_time)],
                [
                  "signals",
                  <div>
                    {info.data.signals.map((s) => (
                      <div key={s.signal} style={{ margin: "2px 0" }}>
                        <Chip on={s.queryable}>{s.signal}</Chip> {s.backend}
                        {s.queryable && s.bind ? (
                          <span style={{ color: "var(--muted-2)" }}> {s.bind}</span>
                        ) : null}
                      </div>
                    ))}
                  </div>,
                ],
              ]}
            />
          ) : null}
        </Card>

        <Card
          title="Runtime"
          sub={runtime.data ? fmtNum(runtime.data.goroutines) + " goroutines" : undefined}
        >
          {runtime.isLoading ? (
            <Spinner />
          ) : runtime.error ? (
            <ErrorBox error={runtime.error} />
          ) : runtime.data ? (
            <>
              {runtime.data.mem_limit_bytes ? (
                <Bar
                  label="heap vs GOMEMLIMIT"
                  value={`${fmtBytes(runtime.data.heap_alloc_bytes)} / ${fmtBytes(runtime.data.mem_limit_bytes)}`}
                  ratio={runtime.data.heap_alloc_bytes / runtime.data.mem_limit_bytes}
                />
              ) : null}
              <Bar
                label="heap vs next GC"
                value={`${fmtBytes(runtime.data.heap_alloc_bytes)} / ${fmtBytes(runtime.data.next_gc_bytes)}`}
                ratio={runtime.data.next_gc_bytes ? runtime.data.heap_alloc_bytes / runtime.data.next_gc_bytes : 0}
              />
              <KV
                rows={[
                  ["heap in-use", <Mono>{fmtBytes(runtime.data.heap_inuse_bytes)}</Mono>],
                  ["GC cycles", <Mono>{fmtNum(runtime.data.gc_count)}</Mono>],
                  ["GOMAXPROCS", <Mono>{runtime.data.gomaxprocs + " / " + runtime.data.num_cpu}</Mono>],
                ]}
              />
            </>
          ) : null}
        </Card>

        <Card title="Health" sub={health.data ? <Pill status={health.data.status} /> : undefined}>
          {health.isLoading ? (
            <Spinner />
          ) : health.error ? (
            <ErrorBox error={health.error} />
          ) : health.data ? (
            <div>
              {health.data.components.map((c) => (
                <div key={c.name}>
                  <div className="health-row">
                    <span>
                      <span className="name">{c.name}</span>
                      {c.addr ? <span className="addr">{c.addr}</span> : null}
                    </span>
                    <Pill status={c.status} />
                  </div>
                  {c.error ? <div className="health-err">{c.error}</div> : null}
                </div>
              ))}
            </div>
          ) : null}
        </Card>
      </div>
    </>
  );
}
