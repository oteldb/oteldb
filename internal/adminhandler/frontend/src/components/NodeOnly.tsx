import type { ReactNode } from "react";
import { Alert } from "@gravity-ui/uikit";
import { useGetInfo } from "../api/admin";
import { useSelectedNode } from "../lib/node";
import { Loading } from "./ui";

/**
 * Two of this panel's pages only work against a storage node's own parts, and the cluster
 * aggregator refuses both rather than fanning them out: stream cost attribution would decode each
 * replicated part once per replica to answer one question, and an action mutates the process it
 * reaches, so one that half-succeeds across a cluster needs a partial-failure contract this API
 * does not have.
 *
 * Both refusals are deliberate. What was missing is that the UI could not tell which it was talking
 * to, so it offered the controls either way and let them fail on click. `InstanceInfo.mode` is that
 * discriminator.
 *
 * The aggregator does forward either operation to a member that is named, so what gates these pages
 * is not the backend alone but whether the request has a node to address.
 */
export function useIsCluster(): boolean {
  return useGetInfo().data?.mode === "cluster";
}

/**
 * Renders `children` when the request has a single node to address — on a storage node, or on the
 * aggregator once a member is picked — and an explanation of what to pick otherwise.
 *
 * The page keeps its route rather than being redirected away: these are linkable, and a bookmark
 * that silently lands on Overview reads as the link having rotted. While info is still loading it
 * shows the spinner, so the explanation never flashes on a node.
 */
export function NodeOnly({ what, children }: { what: string; children: ReactNode }) {
  const info = useGetInfo();
  const { node } = useSelectedNode();

  if (info.isLoading) return <Loading />;
  if (info.data?.mode !== "cluster" || node != null) return <>{children}</>;

  return (
    <Alert
      theme="info"
      view="outlined"
      title="Pick a node"
      message={
        `${what} reads one node's own parts, and this panel is addressing the whole cluster, which ` +
        `holds none of its own. Choose a member in the node picker to run it there.`
      }
    />
  );
}
