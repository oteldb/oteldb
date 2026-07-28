import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Button, Flex, Icon, Text, useToaster } from "@gravity-ui/uikit";
import { BroomMotion, TrashBin, Wrench } from "@gravity-ui/icons";
import type { IconData } from "@gravity-ui/uikit";
import { useGetInfo, useRunAction } from "../api/admin";
import { Panel } from "../components/ui";
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
    hint: "Forces a merge and flush pass on the embedded engine.",
    needsStorage: true,
  },
  {
    action: "gc",
    label: "Run GC",
    icon: BroomMotion,
    hint: "Triggers one Go garbage collection cycle.",
  },
  {
    action: "free-os-memory",
    label: "Free OS memory",
    icon: TrashBin,
    hint: "Returns freed heap to the operating system.",
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
    <Panel title="Actions" sub="Each runs immediately on this instance">
      <Flex gap={6} wrap>
        {ACTIONS.map((a) => {
          const unavailable = a.needsStorage && !storageEnabled;
          return (
            <Flex key={a.action} direction="column" gap={2} maxWidth={280}>
              <Button
                view="outlined"
                size="l"
                width="max"
                loading={pending === a.action}
                disabled={pending != null || unavailable}
                onClick={() => onRun(a.action)}
              >
                <Icon data={a.icon} />
                {a.label}
              </Button>
              <Text variant="caption-2" color={unavailable ? "hint" : "secondary"}>
                {unavailable ? "Needs the embedded engine, which is not active." : a.hint}
              </Text>
            </Flex>
          );
        })}
      </Flex>
    </Panel>
  );
}
