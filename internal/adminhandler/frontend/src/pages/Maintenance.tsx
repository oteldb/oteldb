import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useRunAction, useGetInfo } from "../api/admin";
import { Card, Spinner } from "../components/ui";
import { useToast } from "../components/toast";
import { fmtBytes } from "../lib/format";
import type { ActionName } from "../api/model";

const ACTIONS: { action: ActionName; label: string; hint: string; needsStorage?: boolean }[] = [
  { action: "storage-maintain", label: "Run storage maintenance", hint: "Force a merge/flush pass on the embedded engine.", needsStorage: true },
  { action: "gc", label: "Run GC", hint: "Trigger a Go garbage collection cycle." },
  { action: "free-os-memory", label: "Free OS memory", hint: "Return freed heap to the operating system." },
];

export function Maintenance() {
  const info = useGetInfo();
  const qc = useQueryClient();
  const toast = useToast();
  const run = useRunAction();
  const [note, setNote] = useState<{ text: string; err: boolean }>({ text: "", err: false });
  const [pending, setPending] = useState<ActionName | null>(null);

  const storageEnabled = info.data?.storage_enabled ?? false;

  async function onRun(action: ActionName) {
    setPending(action);
    try {
      const r = await run.mutateAsync({ action });
      const freed = r.freed_bytes != null ? ` (freed ${fmtBytes(r.freed_bytes)})` : "";
      setNote({ text: r.message + freed, err: false });
      toast(r.message + freed);
      // Refresh runtime/storage views affected by the action.
      qc.invalidateQueries({ queryKey: ["/api/v1/runtime"] });
      qc.invalidateQueries({ queryKey: ["/api/v1/storage"] });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setNote({ text: msg, err: true });
      toast(msg, true);
    } finally {
      setPending(null);
    }
  }

  return (
    <>
      <div className="section-title">Maintenance</div>
      <Card title="Runtime controls">
        <div className="actions">
          {ACTIONS.map((a) => {
            const disabled = pending != null || (a.needsStorage && !storageEnabled);
            return (
              <button key={a.action} disabled={disabled} title={a.hint} onClick={() => onRun(a.action)}>
                {pending === a.action ? <Spinner /> : null} {a.label}
              </button>
            );
          })}
        </div>
        <p className={"action-note" + (note.err ? " err" : "")}>{note.text}</p>
      </Card>

      <Card title="What these do" style={{ marginTop: 14 }}>
        <dl className="kv">
          {ACTIONS.map((a) => (
            <div key={a.action} style={{ display: "contents" }}>
              <dt>
                <span className="mono">{a.action}</span>
              </dt>
              <dd style={{ textAlign: "left", color: "var(--muted)" }}>{a.hint}</dd>
            </div>
          ))}
        </dl>
      </Card>
    </>
  );
}
