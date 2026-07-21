import { useEffect, useRef, useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { useGetRuntime } from "../api/admin";
import { Card, Bar, KV, Mono, Spinner, ErrorBox } from "../components/ui";
import { fmtBytes, fmtNum } from "../lib/format";

interface Point {
  t: number;
  heap: number;
  next: number;
}

export function Runtime() {
  const { data, isLoading, error } = useGetRuntime({ query: { refetchInterval: 3_000 } });
  const [series, setSeries] = useState<Point[]>([]);
  const seq = useRef(0);

  useEffect(() => {
    if (!data) return;
    setSeries((prev) => {
      const next = [...prev, { t: seq.current++, heap: data.heap_alloc_bytes, next: data.next_gc_bytes }];
      return next.slice(-60); // keep last ~3 minutes at 3s cadence.
    });
  }, [data]);

  if (isLoading) return <Spinner />;
  if (error) return <ErrorBox error={error} />;
  if (!data) return null;

  return (
    <>
      <div className="section-title">Go runtime</div>
      <Card title="Live heap" sub={fmtNum(data.goroutines) + " goroutines"} wide>
        <div style={{ height: 240, marginTop: 4 }}>
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={series} margin={{ top: 8, right: 12, bottom: 0, left: 4 }}>
              <defs>
                <linearGradient id="heapFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#3b6ef5" stopOpacity={0.35} />
                  <stop offset="100%" stopColor="#3b6ef5" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="#232323" vertical={false} />
              <XAxis dataKey="t" hide />
              <YAxis
                tickFormatter={(v) => fmtBytes(v)}
                width={64}
                tick={{ fill: "#8a8a8a", fontSize: 11 }}
                stroke="#2e2e2e"
              />
              <Tooltip
                contentStyle={{
                  background: "#1a1a1a",
                  border: "1px solid #2e2e2e",
                  borderRadius: 8,
                  color: "#ececec",
                  fontSize: 12,
                }}
                labelFormatter={() => ""}
                formatter={(v: number, name) => [fmtBytes(v), name === "heap" ? "heap alloc" : "next GC"]}
              />
              <Area
                type="monotone"
                dataKey="next"
                stroke="#6a6a6a"
                strokeDasharray="3 3"
                fill="none"
                strokeWidth={1.2}
                isAnimationActive={false}
              />
              <Area
                type="monotone"
                dataKey="heap"
                stroke="#3b6ef5"
                fill="url(#heapFill)"
                strokeWidth={1.8}
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </Card>

      <div className="grid" style={{ marginTop: 14 }}>
        <Card title="Memory">
          {data.mem_limit_bytes ? (
            <Bar
              label="heap vs GOMEMLIMIT"
              value={`${fmtBytes(data.heap_alloc_bytes)} / ${fmtBytes(data.mem_limit_bytes)}`}
              ratio={data.heap_alloc_bytes / data.mem_limit_bytes}
            />
          ) : null}
          <Bar
            label="heap vs next GC"
            value={`${fmtBytes(data.heap_alloc_bytes)} / ${fmtBytes(data.next_gc_bytes)}`}
            ratio={data.next_gc_bytes ? data.heap_alloc_bytes / data.next_gc_bytes : 0}
          />
          <KV
            rows={[
              ["heap alloc", <Mono>{fmtBytes(data.heap_alloc_bytes)}</Mono>],
              ["heap in-use", <Mono>{fmtBytes(data.heap_inuse_bytes)}</Mono>],
              ["heap sys", <Mono>{fmtBytes(data.heap_sys_bytes)}</Mono>],
              ["stack in-use", <Mono>{fmtBytes(data.stack_inuse_bytes)}</Mono>],
            ]}
          />
        </Card>
        <Card title="Scheduler & GC">
          <KV
            rows={[
              ["goroutines", <Mono>{fmtNum(data.goroutines)}</Mono>],
              ["GC cycles", <Mono>{fmtNum(data.gc_count)}</Mono>],
              ["GOMAXPROCS", <Mono>{data.gomaxprocs}</Mono>],
              ["num CPU", <Mono>{data.num_cpu}</Mono>],
              ["GOMEMLIMIT", <Mono>{data.mem_limit_bytes ? fmtBytes(data.mem_limit_bytes) : "unset"}</Mono>],
            ]}
          />
        </Card>
      </div>
    </>
  );
}
