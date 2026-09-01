import type { ReactNode } from "react";
import { Alert } from "@gravity-ui/uikit";
import { useGetInfo } from "../api/admin";
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
 */
export function useIsCluster(): boolean {
  return useGetInfo().data?.mode === "cluster";
}

/**
 * Renders `children` on a storage node, and an explanation on the aggregator.
 *
 * The page keeps its route rather than being redirected away: these are linkable, and a bookmark
 * that silently lands on Overview reads as the link having rotted. While info is still loading it
 * shows the spinner, so the explanation never flashes on a node.
 */
export function NodeOnly({ what, children }: { what: string; children: ReactNode }) {
  const info = useGetInfo();

  if (info.isLoading) return <Loading />;
  if (info.data?.mode !== "cluster") return <>{children}</>;

  return (
    <Alert
      theme="info"
      view="outlined"
      title={`${what} is a per-node view`}
      message={
        `This is the cluster panel, which aggregates every member and holds no parts of its own. ` +
        `Open a storage node's own admin panel to use it.`
      }
    />
  );
}
