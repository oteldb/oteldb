import { useState } from "react";
import { Alert, Button, Flex, Icon, Select, Table, Text, TextInput } from "@gravity-ui/uikit";
import type { TableColumnConfig } from "@gravity-ui/uikit";
import { Magnifier } from "@gravity-ui/icons";
import { useGetStreamCosts } from "../api/admin";
import { Chip, ErrorAlert, head, Mono, Panel, Rule } from "../components/ui";
import { fmtBytes, fmtNum } from "../lib/format";
import type { ColumnCost, RecordSignal, StreamCost } from "../api/model";

const SIGNALS: { value: RecordSignal; content: string }[] = [
  { value: "logs", content: "Logs" },
  { value: "traces", content: "Traces" },
  { value: "profiles", content: "Profiles" },
];

/**
 * A group whose distinct count is large while its digit-normalized distinct is tiny is a
 * mis-parsed source: one templated line with an embedded timestamp or id, never turned into
 * fields. That is a fixable diagnosis rather than a number to stare at, so it is called out
 * instead of left for the reader to divide.
 */
function misparsed(c: ColumnCost): boolean {
  return c.distinct >= 100 && c.distinct_normalized > 0 && c.distinct >= c.distinct_normalized * 20;
}

const COLUMN_COLUMNS: TableColumnConfig<ColumnCost>[] = [
  { id: "name", name: head("column"), primary: true, template: (c) => <Mono>{c.name}</Mono> },
  { id: "raw_bytes", name: head("raw"), align: "end", template: (c) => fmtBytes(c.raw_bytes) },
  { id: "disk_bytes", name: head("disk ≈"), align: "end", template: (c) => fmtBytes(c.disk_bytes) },
  {
    id: "distinct",
    name: head("distinct"),
    align: "end",
    template: (c) => (c.distinct > 0 ? fmtNum(c.distinct) : "—"),
  },
  {
    id: "distinct_normalized",
    name: head("normalized"),
    align: "end",
    template: (c) => (
      <Flex alignItems="center" gap={2} justifyContent="flex-end">
        {c.distinct_normalized > 0 ? fmtNum(c.distinct_normalized) : "—"}
        {misparsed(c) ? <Chip theme="warning">unparsed</Chip> : null}
      </Flex>
    ),
  },
];

function Group({ g, groupBy }: { g: StreamCost; groupBy: string }) {
  return (
    <Panel
      title={g.key || (groupBy ? `(no ${groupBy})` : "(unkeyed stream)")}
      sub={`${fmtNum(g.rows)} rows · ${fmtNum(g.streams)} streams · ${fmtBytes(g.raw_bytes)} raw · ${fmtBytes(g.disk_bytes)} on disk (approx)`}
      actions={!g.distinct_estimated ? <Chip>distinct not measured</Chip> : null}
      scroll
    >
      <Table data={g.columns} columns={COLUMN_COLUMNS} getRowId="name" emptyMessage="No columns." />
    </Panel>
  );
}

export function StreamCosts() {
  const [signal, setSignal] = useState<RecordSignal>("logs");
  const [groupBy, setGroupBy] = useState("service.name");
  // Draft state is what the form edits; the query runs against the values committed by Analyze.
  const [query, setQuery] = useState<{ signal: RecordSignal; groupBy: string } | null>(null);

  const costs = useGetStreamCosts(
    { signal: query?.signal ?? signal, group_by: query?.groupBy ?? groupBy },
    // This decodes every accounted byte column of every live part, so it runs only when asked:
    // no refetch on mount, focus or interval.
    {
      query: {
        enabled: query != null,
        refetchOnWindowFocus: false,
        staleTime: Infinity,
        gcTime: 0,
      },
    },
  );

  const data = costs.data;

  return (
    <Flex direction="column" gap={5}>
      <Flex direction="column" gap={3}>
        <Rule>Stream costs</Rule>
        <Panel
          title="Attribute stored bytes"
          sub="Reads and decodes every accounted byte column of every live part — run it on demand, not on a schedule"
        >
          <Flex gap={4} alignItems="flex-end" wrap>
            <Flex direction="column" gap={2}>
              <Text variant="caption-2" color="secondary">
                signal
              </Text>
              <Select
                value={[signal]}
                options={SIGNALS}
                width={160}
                onUpdate={([v]) => setSignal(v as RecordSignal)}
              />
            </Flex>
            <Flex direction="column" gap={2}>
              <Text variant="caption-2" color="secondary">
                group by label
              </Text>
              <TextInput
                value={groupBy}
                onUpdate={setGroupBy}
                placeholder="service.name — empty groups by stream id"
                size="m"
                style={{ width: 320 }}
              />
            </Flex>
            <Button
              view="action"
              size="m"
              loading={costs.isFetching}
              onClick={() => {
                const same = query?.signal === signal && query?.groupBy === groupBy;
                setQuery({ signal, groupBy });
                // New params fetch on their own once the query key changes; only a re-run of the
                // same ones needs a refetch, and asking for both would run this twice.
                if (same) void costs.refetch();
              }}
            >
              <Icon data={Magnifier} />
              Analyze
            </Button>
          </Flex>
        </Panel>
      </Flex>

      {costs.error ? <ErrorAlert error={costs.error} what="stream costs" /> : null}

      {data && !data.storage_enabled ? (
        <Alert
          theme="info"
          view="outlined"
          title="The embedded engine is not active"
          message="Stream costs are attributed from the embedded engine's parts."
        />
      ) : null}

      {data?.storage_enabled && data.groups.length === 0 ? (
        <Alert
          theme="info"
          view="outlined"
          title="Nothing to attribute"
          message={`No flushed ${data.signal} parts for this tenant yet.`}
        />
      ) : null}

      {data?.groups.map((g) => (
        <Group key={g.key} g={g} groupBy={data.group_by ?? ""} />
      ))}
    </Flex>
  );
}
