import { Col, Flex, Row, Text } from "@gravity-ui/uikit";
import type { ColProps } from "@gravity-ui/uikit";
import { useGetHealth, useGetInfo, useGetRuntime } from "../api/admin";
import {
  Chip,
  KV,
  Mono,
  Panel,
  QueryState,
  SectionTitle,
  StatusLabel,
  UsageBar,
} from "../components/ui";
import { fmtBytes, fmtNum, fmtTime } from "../lib/format";
import { ComponentList } from "./Health";

// One card per row on phones, two from tablet, three on wide screens.
const COL: ColProps["size"] = [12, { m: 6, xl: 4 }];

export function Overview() {
  const info = useGetInfo({ query: { refetchInterval: 10_000 } });
  const runtime = useGetRuntime({ query: { refetchInterval: 5_000 } });
  const health = useGetHealth({ query: { refetchInterval: 10_000 } });

  return (
    <>
      <SectionTitle>Overview</SectionTitle>
      <Row space="4" spaceRow="4">
        <Col size={COL}>
          <Panel title="Instance">
            <QueryState query={info}>
              {(data) => (
                <KV
                  rows={[
                    [
                      "storage",
                      <Chip on>
                        {data.storage_enabled
                          ? `storage:${data.storage_backend || "?"}`
                          : data.clickhouse_enabled
                            ? "clickhouse"
                            : "—"}
                      </Chip>,
                    ],
                    ["platform", <Mono>{`${data.os}/${data.arch}`}</Mono>],
                    ["commit", <Mono>{(data.commit || "—").slice(0, 12)}</Mono>],
                    ["started", fmtTime(data.start_time)],
                    [
                      "signals",
                      <Flex direction="column" gap={1}>
                        {data.signals.map((s) => (
                          <Flex key={s.signal} alignItems="center" gap={2} wrap>
                            <Chip on={s.queryable}>{s.signal}</Chip>
                            <Text variant="body-1">{s.backend}</Text>
                            {s.queryable && s.bind ? (
                              <Text variant="code-inline-1" color="secondary">
                                {s.bind}
                              </Text>
                            ) : null}
                          </Flex>
                        ))}
                      </Flex>,
                    ],
                  ]}
                />
              )}
            </QueryState>
          </Panel>
        </Col>

        <Col size={COL}>
          <Panel
            title="Runtime"
            sub={runtime.data ? `${fmtNum(runtime.data.goroutines)} goroutines` : undefined}
          >
            <QueryState query={runtime}>
              {(data) => (
                <Flex direction="column" gap={4}>
                  {data.mem_limit_bytes ? (
                    <UsageBar
                      label="heap vs GOMEMLIMIT"
                      value={`${fmtBytes(data.heap_alloc_bytes)} / ${fmtBytes(data.mem_limit_bytes)}`}
                      ratio={data.heap_alloc_bytes / data.mem_limit_bytes}
                    />
                  ) : null}
                  <UsageBar
                    label="heap vs next GC"
                    value={`${fmtBytes(data.heap_alloc_bytes)} / ${fmtBytes(data.next_gc_bytes)}`}
                    ratio={data.next_gc_bytes ? data.heap_alloc_bytes / data.next_gc_bytes : 0}
                  />
                  <KV
                    rows={[
                      ["heap in-use", <Mono>{fmtBytes(data.heap_inuse_bytes)}</Mono>],
                      ["GC cycles", <Mono>{fmtNum(data.gc_count)}</Mono>],
                      ["GOMAXPROCS", <Mono>{`${data.gomaxprocs} / ${data.num_cpu}`}</Mono>],
                    ]}
                  />
                </Flex>
              )}
            </QueryState>
          </Panel>
        </Col>

        <Col size={COL}>
          <Panel
            title="Health"
            actions={health.data ? <StatusLabel status={health.data.status} /> : undefined}
          >
            <QueryState query={health}>
              {(data) => <ComponentList components={data.components} />}
            </QueryState>
          </Panel>
        </Col>
      </Row>
    </>
  );
}
