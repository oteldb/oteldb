import { useEffect } from "react";
import { useGetInfo, useGetRuntime, useGetStorage } from "../api/admin";
import {
  clearVital,
  latest,
  recordLevel,
  recordRate,
  useVitalHistory,
  type VitalKey,
} from "../lib/vitals";
import { fmtBytes, fmtDur, fmtNum } from "../lib/format";

const SPARK_W = 64;
const SPARK_H = 22;

/** Trend line for one vital: no axes, no labels — shape only. */
function Spark({ points }: { points: number[] }) {
  if (points.length < 2) {
    return <svg className="spark" width={SPARK_W} height={SPARK_H} aria-hidden="true" />;
  }

  const min = Math.min(...points);
  const max = Math.max(...points);
  // A flat series would divide by zero; draw it down the middle instead.
  const span = max - min || 1;
  const flat = max === min;
  const step = SPARK_W / (points.length - 1);
  const d = points
    .map((v, i) => {
      const x = i * step;
      const y = flat
        ? SPARK_H / 2
        : SPARK_H - 1 - ((v - min) / span) * (SPARK_H - 2);
      return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  return (
    <svg className="spark" width={SPARK_W} height={SPARK_H} aria-hidden="true">
      <path className="spark__area" d={`${d} L${SPARK_W},${SPARK_H} L0,${SPARK_H} Z`} />
      <path className="spark__line" d={d} />
    </svg>
  );
}

function Cell({
  label,
  value,
  unit,
  points,
}: {
  label: string;
  value: string;
  unit?: string;
  points?: number[];
}) {
  return (
    <div className="vitals__cell">
      <span className="vitals__label">{label}</span>
      <div className="vitals__reading">
        <span className="vitals__value">
          {value}
          {unit ? <span className="vitals__unit">{unit}</span> : null}
        </span>
        {points ? <Spark points={points} /> : null}
      </div>
    </div>
  );
}

const DASH = "—";

/**
 * The instrument tape: the five readings that answer "what is this instance
 * doing right now", kept visible on every route. Values come from the polls the
 * pages make anyway — react-query serves both from one request per interval.
 */
export function Vitals() {
  const info = useGetInfo({ query: { refetchInterval: 10_000 } });
  const runtime = useGetRuntime({ query: { refetchInterval: 3_000 } });
  const storage = useGetStorage({ query: { refetchInterval: 10_000 } });
  const history = useVitalHistory();

  useEffect(() => {
    if (!runtime.data) return;
    recordLevel("heap", runtime.data.heap_alloc_bytes, runtime.data);
    recordLevel("goroutines", runtime.data.goroutines, runtime.data);
  }, [runtime.data]);

  useEffect(() => {
    const tenants = storage.data?.engine?.tenants;
    if (!tenants) {
      // Without the embedded engine there is nothing to count; drop any history
      // rather than freezing the last value from a previous configuration.
      clearVital("ingest");
      clearVital("series");
      return;
    }
    const accepted = tenants.reduce((sum, t) => sum + t.admission.accepted, 0);
    const series = tenants.reduce((sum, t) => sum + t.total_series, 0);
    recordRate("ingest", accepted, storage.data);
    recordLevel("series", series, storage.data);
  }, [storage.data]);

  const read = (key: VitalKey) => latest(history[key]);
  const ingest = read("ingest");
  const heap = read("heap");
  const goroutines = read("goroutines");
  const series = read("series");

  return (
    <div className="vitals" role="group" aria-label="Instance vitals">
      <Cell
        label="uptime"
        value={info.data ? fmtDur(info.data.uptime_seconds) : DASH}
      />
      <Cell
        label="ingest"
        value={ingest == null ? DASH : fmtNum(Math.round(ingest))}
        unit={ingest == null ? undefined : "/s"}
        points={history.ingest}
      />
      <Cell
        label="heap"
        value={heap == null ? DASH : fmtBytes(heap)}
        points={history.heap}
      />
      <Cell
        label="goroutines"
        value={goroutines == null ? DASH : fmtNum(goroutines)}
        points={history.goroutines}
      />
      <Cell
        label="series"
        value={series == null ? DASH : fmtNum(series)}
        points={history.series}
      />
    </div>
  );
}
