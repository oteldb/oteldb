import type { ReactNode } from "react";
import {
  Alert,
  Box,
  Card,
  DefinitionList,
  Flex,
  Label,
  Progress,
  Spin,
  Text,
} from "@gravity-ui/uikit";
import type { LabelProps, ProgressColorStops } from "@gravity-ui/uikit";
import { pct } from "../lib/format";
import type { HealthStatus } from "../api/model";

/**
 * Column header in the same machine voice as the vitals tape. Table takes a
 * render function for `name`, so this needs no reach into the table's markup.
 */
export function head(text: string) {
  return () => <span className="label-micro">{text}</span>;
}

/**
 * Boundary between two subsystems on one page — a labelled hairline rather than
 * a heading, because it marks where one thing ends, not what ranks above what.
 */
export function Rule({ children }: { children: ReactNode }) {
  return (
    <div className="rule label-micro">
      <span>{children}</span>
    </div>
  );
}

/** Bordered container with an optional title row; the building block of every page. */
export function Panel({
  title,
  sub,
  actions,
  scroll,
  children,
}: {
  title?: ReactNode;
  sub?: ReactNode;
  actions?: ReactNode;
  scroll?: boolean;
  children: ReactNode;
}) {
  return (
    <Card view="outlined" className="panel">
      <Flex direction="column" gap={4} height="100%">
        {(title != null || actions != null) && (
          <Flex alignItems="baseline" justifyContent="space-between" gap={3}>
            <Flex alignItems="baseline" gap={2} minWidth={0} wrap>
              {title != null && <Text variant="subheader-2">{title}</Text>}
              {sub != null && (
                <Text variant="body-1" color="secondary">
                  {sub}
                </Text>
              )}
            </Flex>
            {actions}
          </Flex>
        )}
        {scroll ? <div className="panel__scroll">{children}</div> : children}
      </Flex>
    </Card>
  );
}

// Utilization ramps from neutral to danger as the bar fills up.
const USAGE_STOPS: ProgressColorStops[] = [
  { theme: "info", stop: 70 },
  { theme: "warning", stop: 90 },
  { theme: "danger", stop: 100 },
];

export function UsageBar({
  label,
  value,
  ratio,
}: {
  label: ReactNode;
  value: ReactNode;
  ratio: number;
}) {
  const p = pct(ratio);
  return (
    <Flex direction="column" gap={1}>
      <Flex alignItems="baseline" justifyContent="space-between" gap={2}>
        <Text variant="body-1" color="secondary">
          {label}
        </Text>
        <Text variant="code-inline-1">{value}</Text>
      </Flex>
      {/* Progress centers itself with `margin: 0 auto`, which collapses its width
          when it is a direct flex child — keep it inside a block wrapper. */}
      <Box>
        <Progress size="s" value={p} colorStops={USAGE_STOPS} />
      </Box>
    </Flex>
  );
}

const HEALTH_THEMES: Record<HealthStatus, LabelProps["theme"]> = {
  healthy: "success",
  degraded: "warning",
  unhealthy: "danger",
};

export function StatusLabel({ status }: { status: HealthStatus }) {
  return <Label theme={HEALTH_THEMES[status] ?? "unknown"}>{status}</Label>;
}

export function Chip({ on, children }: { on?: boolean; children: ReactNode }) {
  return (
    <Label theme={on ? "info" : "unknown"} size="xs">
      {children}
    </Label>
  );
}

export function KV({ rows }: { rows: [ReactNode, ReactNode][] }) {
  return (
    <DefinitionList responsive>
      {rows.map(([name, value], i) => (
        <DefinitionList.Item key={i} name={name}>
          {value}
        </DefinitionList.Item>
      ))}
    </DefinitionList>
  );
}

export function Mono({ children }: { children: ReactNode }) {
  return <Text variant="code-inline-1">{children}</Text>;
}

/** Monospace number that turns red once it is non-zero (error counters). */
export function ErrCount({ children, bad }: { children: ReactNode; bad: boolean }) {
  return (
    <Text variant="code-inline-1" color={bad ? "danger" : undefined}>
      {children}
    </Text>
  );
}

export function Loading() {
  return (
    <Flex justifyContent="center" spacing={{ p: 5 }}>
      <Spin size="m" />
    </Flex>
  );
}

/** Names what didn't load, then quotes the API verbatim. */
export function ErrorAlert({ error, what }: { error: unknown; what?: string }) {
  const message = error instanceof Error ? error.message : String(error);
  return (
    <Alert
      theme="danger"
      view="outlined"
      title={what ? `Couldn't load ${what}` : "Request failed"}
      message={message}
    />
  );
}

/**
 * Renders the common query states (loading, error, empty) around `children`,
 * so pages don't repeat the same three branches.
 */
export function QueryState<T>({
  query,
  what,
  children,
}: {
  query: { data?: T; isLoading: boolean; error: unknown };
  what?: string;
  children: (data: T) => ReactNode;
}) {
  if (query.isLoading) return <Loading />;
  if (query.error) return <ErrorAlert error={query.error} what={what} />;
  if (!query.data) return null;
  return <>{children(query.data)}</>;
}
