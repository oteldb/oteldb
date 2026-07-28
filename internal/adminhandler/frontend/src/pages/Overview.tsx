import { Alert, Col, Flex, Icon, Row, Table, Text } from "@gravity-ui/uikit";
import type { ColProps, TableColumnConfig } from "@gravity-ui/uikit";
import { CircleCheck } from "@gravity-ui/icons";
import { useGetHealth, useGetInfo } from "../api/admin";
import { head, KV, Mono, Panel, QueryState, Rule } from "../components/ui";
import { fmtTime } from "../lib/format";
import type { HealthReport, SignalInfo } from "../api/model";

// What the instance serves gets the width; what it was built from gets the rest.
const SIGNALS_COL: ColProps["size"] = [12, { l: 8 }];
const BUILD_COL: ColProps["size"] = [12, { l: 4 }];

/**
 * Components that need attention. Nothing renders when everything is healthy
 * beyond a single line — the presence of this block is the signal.
 */
function Exceptions({ health }: { health: HealthReport }) {
  const failing = health.components.filter((c) => c.status !== "healthy");

  if (!failing.length) {
    return (
      <Flex alignItems="center" gap={2}>
        <Icon data={CircleCheck} size={14} color="positive" />
        <Text variant="body-1" color="secondary">
          All {health.components.length} components healthy
        </Text>
      </Flex>
    );
  }

  return (
    <Flex direction="column" gap={2}>
      {failing.map((c) => (
        <Alert
          key={c.name}
          theme={c.status === "unhealthy" ? "danger" : "warning"}
          view="outlined"
          title={`${c.name}${c.addr ? ` (${c.addr})` : ""} is ${c.status}`}
          message={c.error || "The component reported no detail."}
        />
      ))}
    </Flex>
  );
}

const SIGNAL_COLUMNS: TableColumnConfig<SignalInfo>[] = [
  { id: "signal", name: head("signal"), primary: true },
  { id: "backend", name: head("served by") },
  {
    id: "bind",
    name: head("query api"),
    template: (s) => (s.queryable && s.bind ? <Mono>{s.bind}</Mono> : "—"),
  },
  {
    id: "queryable",
    name: head("state"),
    align: "end",
    template: (s) => (
      <Text variant="caption-2" color={s.queryable ? "positive" : "hint"}>
        {s.queryable ? "queryable" : "ingest only"}
      </Text>
    ),
  },
];

export function Overview() {
  const info = useGetInfo({ query: { refetchInterval: 10_000 } });
  const health = useGetHealth({ query: { refetchInterval: 10_000 } });

  return (
    <Flex direction="column" gap={5}>
      <QueryState query={health} what="component health">
        {(data) => <Exceptions health={data} />}
      </QueryState>

      <Row space="4" spaceRow="5">
        <Col size={SIGNALS_COL}>
          <Flex direction="column" gap={3} height="100%">
            <Rule>Signals</Rule>
            <Panel scroll>
              <QueryState query={info} what="instance info">
                {(data) => (
                  <Table
                    data={data.signals}
                    columns={SIGNAL_COLUMNS}
                    getRowId="signal"
                    width="max"
                  />
                )}
              </QueryState>
            </Panel>
          </Flex>
        </Col>

        <Col size={BUILD_COL}>
          <Flex direction="column" gap={3} height="100%">
            <Rule>Build</Rule>
            <Panel>
              <QueryState query={info} what="instance info">
                {(data) => (
                  <KV
                    rows={[
                      [
                        "storage",
                        <Mono>
                          {data.storage_enabled
                            ? `storage:${data.storage_backend || "?"}`
                            : data.clickhouse_enabled
                              ? "clickhouse"
                              : "none"}
                        </Mono>,
                      ],
                      ["version", <Mono>{data.version || "dev"}</Mono>],
                      ["commit", <Mono>{(data.commit || "—").slice(0, 12)}</Mono>],
                      ["go", <Mono>{data.go_version}</Mono>],
                      ["platform", <Mono>{`${data.os}/${data.arch}`}</Mono>],
                      ["started", <Mono>{fmtTime(data.start_time)}</Mono>],
                    ]}
                  />
                )}
              </QueryState>
            </Panel>
          </Flex>
        </Col>
      </Row>
    </Flex>
  );
}
