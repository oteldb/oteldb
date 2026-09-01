import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from "react";

const STORAGE_KEY = "oteldb.admin.node";

function readStoredNode(): string | null {
  try {
    return window.localStorage.getItem(STORAGE_KEY) || null;
  } catch {
    // Private mode or blocked storage: start on the whole cluster.
    return null;
  }
}

interface SelectedNodeContext {
  /** Ring id the panel is addressing, or null for the whole cluster. */
  node: string | null;
  setNode: (node: string | null) => void;
  /** Query parameters that address the selection, spreadable into any generated hook. */
  params: { node?: string };
}

const Ctx = createContext<SelectedNodeContext>({
  node: null,
  setNode: () => {},
  params: {},
});

/**
 * What the panel's requests are addressed to. Null is the whole cluster, which is what an
 * aggregator answers with no `node` parameter and the only thing a storage node can answer at all.
 *
 * It is app-wide state rather than per page because it is the frame every page is read in: a
 * heap figure means something different when it is one node's than when it is the cluster's sum,
 * and a selection that reset on navigation would keep changing which of the two is on screen.
 */
export function useSelectedNode(): SelectedNodeContext {
  return useContext(Ctx);
}

/** Holds the selected node and remembers it across reloads. */
export function SelectedNodeProvider({ children }: { children: ReactNode }) {
  const [node, setNodeState] = useState<string | null>(readStoredNode);

  const setNode = useCallback((next: string | null) => {
    setNodeState(next);
    try {
      if (next == null) window.localStorage.removeItem(STORAGE_KEY);
      else window.localStorage.setItem(STORAGE_KEY, next);
    } catch {
      // Persisting is best-effort.
    }
  }, []);

  const value = useMemo(
    () => ({ node, setNode, params: node == null ? {} : { node } }),
    [node, setNode],
  );

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}
