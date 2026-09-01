import { useEffect, useMemo } from "react";
import { Select, Tooltip } from "@gravity-ui/uikit";
import { useGetClusterNodes } from "../api/admin";
import { useSelectedNode } from "../lib/node";
import { useIsCluster } from "./NodeOnly";

const WHOLE_CLUSTER = "";

/**
 * What the panel's requests are addressed to: the whole cluster, or one of its members.
 *
 * Only the aggregator offers it — a storage node is the only node it could pick — and the list is
 * /api/v1/cluster/nodes rather than the storage report that also carries one, because this runs on
 * a poll and that report reads every part of every node.
 *
 * An unreachable member stays in the list and selectable: what an operator does with a node that
 * stopped answering is look at it, and a picker that hid it would take that away exactly when it
 * is wanted.
 */
export function NodePicker() {
  const isCluster = useIsCluster();
  const { node, setNode } = useSelectedNode();
  const nodes = useGetClusterNodes({
    query: { enabled: isCluster, refetchInterval: 15_000 },
  });

  const members = useMemo(() => nodes.data?.nodes ?? [], [nodes.data]);

  // A member that left the ring cannot answer, and every request would keep naming it. Fall back
  // to the cluster, which is the one target that always exists.
  useEffect(() => {
    if (node == null || members.length === 0) return;
    if (!members.some((m) => m.node === node)) setNode(null);
  }, [node, members, setNode]);

  if (!isCluster) return null;

  const options = [
    { value: WHOLE_CLUSTER, content: "Whole cluster" },
    ...members.map((m) => ({
      value: m.node,
      content: m.status === "ok" ? m.node : `${m.node} (unreachable)`,
    })),
  ];

  return (
    <Tooltip
      content={
        node == null
          ? "Every request aggregates all members. Pick one to address it directly."
          : `Requests are addressed to ${node}.`
      }
    >
      <Select
        value={[node ?? WHOLE_CLUSTER]}
        options={options}
        width={200}
        size="m"
        loading={nodes.isLoading}
        aria-label="Node the panel addresses"
        onUpdate={([v]) => setNode(v === WHOLE_CLUSTER ? null : v)}
      />
    </Tooltip>
  );
}
