import { NavLink, Route, Routes } from "react-router-dom";
import { useIsFetching, useQueryClient } from "@tanstack/react-query";
import { useGetInfo } from "./api/admin";
import { fmtDur } from "./lib/format";
import MatrixRain from "./components/MatrixRain";
import { Overview } from "./pages/Overview";
import { Runtime } from "./pages/Runtime";
import { Health } from "./pages/Health";
import { Storage } from "./pages/Storage";
import { Maintenance } from "./pages/Maintenance";

const NAV = [
  { to: "/", label: "Overview", end: true },
  { to: "/runtime", label: "Runtime" },
  { to: "/health", label: "Health" },
  { to: "/storage", label: "Storage" },
  { to: "/maintenance", label: "Maintenance" },
];

function Dot({ className }: { className: string }) {
  return (
    <svg className={className} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="8" cy="8" r="6" stroke="currentColor" strokeWidth="1.4" />
    </svg>
  );
}

export default function App() {
  const info = useGetInfo({ query: { refetchInterval: 10_000 } });
  const qc = useQueryClient();
  const fetching = useIsFetching();

  const version = info.data ? (info.data.version || "dev") + " · " + info.data.go_version : "";
  const uptime = info.data ? "up " + fmtDur(info.data.uptime_seconds) : "";

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="logo">oteldb</span>
          <span className="tag">admin</span>
        </div>
        <div className="nav-group">Instance</div>
        {NAV.map((n) => (
          <NavLink
            key={n.to}
            to={n.to}
            end={n.end}
            className={({ isActive }) => "nav-item" + (isActive ? " active" : "")}
          >
            <Dot className="ic" />
            {n.label}
          </NavLink>
        ))}
        <div className="foot">{version}</div>
      </aside>

      <div className="content">
        <header className="topbar">
          <MatrixRain />
          <div className="topbar__scrim" aria-hidden="true" />
          <div className="topbar__content">
            <h1>oteldb</h1>
            <span className="sub">admin panel</span>
            <span className="meta">{uptime}</span>
            <div className="spacer" />
            <span className="meta">{version}</span>
            <button
              disabled={fetching > 0}
              onClick={() => qc.invalidateQueries()}
            >
              {fetching > 0 ? "Refreshing…" : "Refresh"}
            </button>
          </div>
        </header>
        <main className="page">
          <Routes>
            <Route path="/" element={<Overview />} />
            <Route path="/runtime" element={<Runtime />} />
            <Route path="/health" element={<Health />} />
            <Route path="/storage" element={<Storage />} />
            <Route path="/maintenance" element={<Maintenance />} />
          </Routes>
        </main>
      </div>
    </div>
  );
}
