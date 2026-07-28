import { useSyncExternalStore } from "react";

/**
 * Rolling history behind the vitals tape.
 *
 * The panel already polls /info, /runtime and /storage; this keeps the last few
 * minutes of what those responses reported so the tape can draw a trend instead
 * of a bare number. It lives outside React so the history survives navigation —
 * the tape is mounted app-wide and must not restart every time a route changes.
 */

export type VitalKey = "ingest" | "heap" | "goroutines" | "series";

/** Samples retained per vital (~3 minutes at the 3 s runtime cadence). */
const WINDOW = 60;

const history: Record<VitalKey, number[]> = {
  ingest: [],
  heap: [],
  goroutines: [],
  series: [],
};

// Sources are react-query results, which keep a stable identity while the data
// is unchanged. Recording against the identity keeps duplicate renders (and
// StrictMode's double effects) from stuttering the history.
const lastSource = new Map<VitalKey, unknown>();

const listeners = new Set<() => void>();
let snapshot: Record<VitalKey, number[]> = { ...history };

function publish() {
  snapshot = { ...history };
  for (const listener of listeners) listener();
}

function push(key: VitalKey, value: number) {
  const series = history[key];
  series.push(value);
  if (series.length > WINDOW) series.splice(0, series.length - WINDOW);
}

/** Records a level (heap bytes, goroutine count, …) as reported. */
export function recordLevel(key: VitalKey, value: number, source: unknown) {
  if (lastSource.get(key) === source) return;
  lastSource.set(key, source);
  push(key, value);
  publish();
}

const counters = new Map<VitalKey, { total: number; at: number }>();

/**
 * Records a monotonic counter as a per-second rate. The first reading only
 * establishes a baseline; a counter that moved backwards means the process
 * restarted, so the history restarts with it.
 */
export function recordRate(key: VitalKey, total: number, source: unknown) {
  if (lastSource.get(key) === source) return;
  lastSource.set(key, source);

  const now = Date.now();
  const prev = counters.get(key);
  counters.set(key, { total, at: now });
  if (!prev) return;

  const seconds = (now - prev.at) / 1000;
  if (seconds < 0.5) return;
  if (total < prev.total) {
    history[key] = [];
    publish();
    return;
  }
  push(key, (total - prev.total) / seconds);
  publish();
}

/** Forgets a vital the current instance cannot report (e.g. no engine). */
export function clearVital(key: VitalKey) {
  if (history[key].length === 0 && !counters.has(key)) return;
  history[key] = [];
  counters.delete(key);
  lastSource.delete(key);
  publish();
}

export function useVitalHistory(): Record<VitalKey, number[]> {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    () => snapshot,
    () => snapshot,
  );
}

export function latest(series: number[]): number | undefined {
  return series.length ? series[series.length - 1] : undefined;
}
