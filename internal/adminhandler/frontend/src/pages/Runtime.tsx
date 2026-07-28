import { useEffect, useMemo, useState } from "react";
import { Col, Flex, Row, sp, useThemeType } from "@gravity-ui/uikit";
import type { ColProps } from "@gravity-ui/uikit";
import { Chart, FORMAT_UNITS_BYTES } from "@gravity-ui/charts";
import type { ChartData } from "@gravity-ui/charts";
import { useGetRuntime } from "../api/admin";
import { ErrorAlert, KV, Loading, Mono, Panel, SectionTitle, UsageBar } from "../components/ui";
import { fmtBytes, fmtNum } from "../lib/format";

interface Point {
  t: number;
  heap: number;
  next: number;
}

// Poll cadence of the runtime endpoint and the retained window of the chart.
const POLL_MS = 3_000;
const KEEP_POINTS = 60;

const COL: ColProps["size"] = [12, { l: 6 }];

// go-faster stops (see brand.css); the chart paints SVG, so it needs literal
// colors rather than the --g-* tokens the rest of the UI reads.
const SERIES_COLORS = {
  dark: { heap: "#01add8", next: "#74d9f2" },
  light: { heap: "#0090bd", next: "#00a29c" },
};

export function Runtime() {
  const { data, isLoading, error } = useGetRuntime({ query: { refetchInterval: POLL_MS } });
  const [series, setSeries] = useState<Point[]>([]);
  const colors = SERIES_COLORS[useThemeType()];

  useEffect(() => {
    if (!data) return;
    setSeries((prev) =>
      [
        ...prev,
        { t: Date.now(), heap: data.heap_alloc_bytes, next: data.next_gc_bytes },
      ].slice(-KEEP_POINTS),
    );
  }, [data]);

  const chartData = useMemo<ChartData>(() => {
    // The chart formats datetime axes in UTC, which reads wrong for a live
    // window — plot age in seconds instead, ending at "now".
    const last = series.length ? series[series.length - 1].t : 0;
    const age = (p: Point) => Math.round((p.t - last) / 1000);
    return {
      series: {
        data: [
          {
            type: "area",
            name: "heap alloc",
            color: colors.heap,
            data: series.map((p) => ({ x: age(p), y: p.heap })),
          },
          {
            type: "line",
            name: "next GC",
            color: colors.next,
            dashStyle: "Dash",
            data: series.map((p) => ({ x: age(p), y: p.next })),
          },
        ],
      },
      xAxis: { type: "linear", labels: { numberFormat: { postfix: " s" } } },
      yAxis: [
        {
          min: 0,
          labels: { numberFormat: { units: FORMAT_UNITS_BYTES, precision: 1 } },
        },
      ],
      tooltip: {
        valueFormat: { type: "number", units: FORMAT_UNITS_BYTES, precision: 1 },
        headerFormat: { type: "number", postfix: " s" },
      },
      legend: { enabled: true },
    };
  }, [series, colors]);

  if (isLoading) return <Loading />;
  if (error) return <ErrorAlert error={error} />;
  if (!data) return null;

  return (
    <>
      <SectionTitle>Go runtime</SectionTitle>
      <Panel title="Live heap" sub={`${fmtNum(data.goroutines)} goroutines`}>
        <div className="chart">
          {/* Chart rejects an empty dataset, so wait for the first sample. */}
          {series.length ? <Chart data={chartData} /> : <Loading />}
        </div>
      </Panel>

      <Row space="4" spaceRow="4" className={sp({ mt: 4 })}>
        <Col size={COL}>
          <Panel title="Memory">
            <Flex direction="column" gap={4}>
              {data.mem_limit_bytes ? (
                <UsageBar
                  label="heap vs GOMEMLIMIT"
                  value={`${fmtBytes(data.heap_alloc_bytes)} / ${fmtBytes(data.mem_limit_bytes)}`}
                  ratio={data.heap_alloc_bytes / data.mem_limit_bytes}
                />
              ) : null}
              <UsageBar
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
            </Flex>
          </Panel>
        </Col>
        <Col size={COL}>
          <Panel title="Scheduler & GC">
            <KV
              rows={[
                ["goroutines", <Mono>{fmtNum(data.goroutines)}</Mono>],
                ["GC cycles", <Mono>{fmtNum(data.gc_count)}</Mono>],
                ["GOMAXPROCS", <Mono>{data.gomaxprocs}</Mono>],
                ["num CPU", <Mono>{data.num_cpu}</Mono>],
                [
                  "GOMEMLIMIT",
                  <Mono>{data.mem_limit_bytes ? fmtBytes(data.mem_limit_bytes) : "unset"}</Mono>,
                ],
              ]}
            />
          </Panel>
        </Col>
      </Row>
    </>
  );
}
