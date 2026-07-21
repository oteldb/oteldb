import type { ReactNode } from "react";
import { pct } from "../lib/format";
import type { HealthStatus } from "../api/model";

export function Card({
  title,
  sub,
  wide,
  scroll,
  children,
  style,
}: {
  title?: ReactNode;
  sub?: ReactNode;
  wide?: boolean;
  scroll?: boolean;
  children: ReactNode;
  style?: React.CSSProperties;
}) {
  return (
    <section className={["card", wide ? "wide" : "", scroll ? "scroll" : ""].join(" ").trim()} style={style}>
      {title != null && (
        <h2>
          {title}
          {sub != null && <span className="sub">{sub}</span>}
        </h2>
      )}
      {children}
    </section>
  );
}

export function Bar({ label, value, ratio }: { label: ReactNode; value: ReactNode; ratio: number }) {
  const p = pct(ratio);
  const cls = p >= 90 ? "crit" : p >= 70 ? "warn" : "";
  return (
    <div className="bar-row">
      <div className="bar-head">
        <span className="label">{label}</span>
        <span className="value">{value}</span>
      </div>
      <div className={`bar ${cls}`}>
        <span style={{ width: `${p.toFixed(1)}%` }} />
      </div>
    </div>
  );
}

export function Pill({ status }: { status: HealthStatus }) {
  return <span className={`pill ${status}`}>{status}</span>;
}

export function Chip({ on, children }: { on?: boolean; children: ReactNode }) {
  return <span className={`chip ${on ? "on" : ""}`}>{children}</span>;
}

export function KV({ rows }: { rows: [ReactNode, ReactNode][] }) {
  return (
    <dl className="kv">
      {rows.map(([k, v], i) => (
        <div key={i} style={{ display: "contents" }}>
          <dt>{k}</dt>
          <dd>{v}</dd>
        </div>
      ))}
    </dl>
  );
}

export function Mono({ children }: { children: ReactNode }) {
  return <span className="mono">{children}</span>;
}

export function Spinner() {
  return <span className="spin" aria-label="loading" />;
}

export function ErrorBox({ error }: { error: unknown }) {
  const msg = error instanceof Error ? error.message : String(error);
  return <div className="err-box">Failed to load: {msg}</div>;
}
