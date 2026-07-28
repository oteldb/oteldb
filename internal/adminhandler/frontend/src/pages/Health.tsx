import { Flex, Text } from "@gravity-ui/uikit";
import { useGetHealth } from "../api/admin";
import { Panel, QueryState, StatusLabel } from "../components/ui";
import type { ComponentHealth } from "../api/model";

/** Component rows with their status; shared with the Overview page. */
export function ComponentList({ components }: { components: ComponentHealth[] }) {
  return (
    <Flex direction="column" gap={3}>
      {components.map((c) => (
        <Flex key={c.name} direction="column" gap={1}>
          <Flex alignItems="center" justifyContent="space-between" gap={3}>
            <Flex alignItems="baseline" gap={2} minWidth={0} wrap>
              <Text variant="body-2">{c.name}</Text>
              {c.addr ? (
                <Text variant="code-inline-1" color="secondary">
                  {c.addr}
                </Text>
              ) : null}
            </Flex>
            <StatusLabel status={c.status} />
          </Flex>
          {c.error ? (
            <Text variant="body-1" color="danger">
              {c.error}
            </Text>
          ) : null}
        </Flex>
      ))}
    </Flex>
  );
}

export function Health() {
  const health = useGetHealth({ query: { refetchInterval: 5_000 } });

  return (
    <Panel
      title="Services"
      actions={health.data ? <StatusLabel status={health.data.status} /> : undefined}
    >
      <QueryState query={health} what="component health">
        {(data) => <ComponentList components={data.components} />}
      </QueryState>
    </Panel>
  );
}
