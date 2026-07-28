import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Button, Flex, Icon, sp, Text, useToaster } from "@gravity-ui/uikit";
import { BroomMotion, TrashBin, Wrench } from "@gravity-ui/icons";
import type { IconData } from "@gravity-ui/uikit";
import { useGetInfo, useRunAction } from "../api/admin";
import { KV, Mono, Panel, SectionTitle } from "../components/ui";
import { fmtBytes } from "../lib/format";
import type { ActionName } from "../api/model";

const ACTIONS: {
  action: ActionName;
  label: string;
  icon: IconData;
  hint: string;
  needsStorage?: boolean;
}[] = [
  {
    action: "storage-maintain",
    label: "Run storage maintenance",
    icon: Wrench,
    hint: "Force a merge/flush pass on the embedded engine.",
    needsStorage: true,
  },
  { action: "gc", label: "Run GC", icon: BroomMotion, hint: "Trigger a Go garbage collection cycle." },
  {
    action: "free-os-memory",
    label: "Free OS memory",
    icon: TrashBin,
    hint: "Return freed heap to the operating system.",
  },
];

export function Maintenance() {
  const info = useGetInfo();
  const qc = useQueryClient();
  const toaster = useToaster();
  const run = useRunAction();
  const [pending, setPending] = useState<ActionName | null>(null);

  const storageEnabled = info.data?.storage_enabled ?? false;

  async function onRun(action: ActionName) {
    setPending(action);
    try {
      const r = await run.mutateAsync({ action });
      const freed = r.freed_bytes != null ? ` (freed ${fmtBytes(r.freed_bytes)})` : "";
      toaster.add({
        name: `action-${action}`,
        theme: "success",
        title: action,
        content: r.message + freed,
      });
      // Refresh runtime/storage views affected by the action.
      qc.invalidateQueries({ queryKey: ["/api/v1/runtime"] });
      qc.invalidateQueries({ queryKey: ["/api/v1/storage"] });
    } catch (e) {
      toaster.add({
        name: `action-${action}`,
        theme: "danger",
        title: action,
        content: e instanceof Error ? e.message : String(e),
        autoHiding: false,
      });
    } finally {
      setPending(null);
    }
  }

  return (
    <>
      <SectionTitle>Maintenance</SectionTitle>
      <Panel title="Runtime controls">
        <Flex gap={3} wrap>
          {ACTIONS.map((a) => (
            <Button
              key={a.action}
              view="outlined"
              size="l"
              title={a.hint}
              loading={pending === a.action}
              disabled={pending != null || (a.needsStorage && !storageEnabled)}
              onClick={() => onRun(a.action)}
            >
              <Icon data={a.icon} />
              {a.label}
            </Button>
          ))}
        </Flex>
        {!storageEnabled && (
          <Text variant="body-1" color="secondary">
            Storage maintenance is unavailable: the embedded engine is not active.
          </Text>
        )}
      </Panel>

      <div className={sp({ mt: 4 })}>
        <Panel title="What these do">
          <KV rows={ACTIONS.map((a) => [<Mono>{a.action}</Mono>, a.hint])} />
        </Panel>
      </div>
    </>
  );
}
