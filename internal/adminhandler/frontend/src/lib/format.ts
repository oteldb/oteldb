// Formatting helpers shared across pages.

export function fmtBytes(n: number | null | undefined): string {
  if (n == null) return "—";
  let v = Number(n);
  const neg = v < 0;
  v = Math.abs(v);
  const u = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let i = 0;
  while (v >= 1024 && i < u.length - 1) {
    v /= 1024;
    i++;
  }
  return (neg ? "-" : "") + v.toFixed(i ? 1 : 0) + " " + u[i];
}

export function fmtNum(n: number | null | undefined): string {
  return n == null ? "—" : Number(n).toLocaleString();
}

export function fmtTime(s: string | null | undefined): string {
  return s ? new Date(s).toLocaleString() : "—";
}

export function fmtDur(secondsInput: number): string {
  const sec = Math.floor(secondsInput);
  const d = Math.floor(sec / 86400);
  const h = Math.floor((sec % 86400) / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  const parts = [d && d + "d", h && h + "h", m && m + "m", s + "s"].filter(Boolean) as string[];
  return parts.slice(0, 2).join(" ") || "0s";
}

export function pct(ratio: number): number {
  return Math.max(0, Math.min(100, ratio * 100));
}
