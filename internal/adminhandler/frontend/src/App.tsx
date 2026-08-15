import { lazy, Suspense, useCallback, useMemo, useState } from "react";
import { Navigate, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import { useIsFetching, useQueryClient } from "@tanstack/react-query";
import { AsideHeader, type AsideHeaderItem } from "@gravity-ui/navigation";
import { Button, Flex, Icon, Text, Tooltip } from "@gravity-ui/uikit";
import {
  ArrowsRotateRight,
  ChartMixed,
  Cpu,
  Database,
  Display,
  HeartPulse,
  Layers3Diagonal,
  Moon,
  Sun,
  Wrench,
} from "@gravity-ui/icons";
import { GoFasterMark } from "./components/GoFasterMark";
import type { Theme } from "@gravity-ui/uikit";
import { useGetInfo } from "./api/admin";
import { useAppTheme } from "./lib/theme";
import { Loading } from "./components/ui";
import { Vitals } from "./components/Vitals";

// Pages are split out of the initial bundle; the chart library on Runtime is
// the bulk of the app's JavaScript and is only fetched when that page opens.
const Overview = lazy(() => import("./pages/Overview").then((m) => ({ default: m.Overview })));
const Runtime = lazy(() => import("./pages/Runtime").then((m) => ({ default: m.Runtime })));
const Health = lazy(() => import("./pages/Health").then((m) => ({ default: m.Health })));
const Storage = lazy(() => import("./pages/Storage").then((m) => ({ default: m.Storage })));
const Maintenance = lazy(() =>
  import("./pages/Maintenance").then((m) => ({ default: m.Maintenance })),
);
const StreamCosts = lazy(() =>
  import("./pages/StreamCosts").then((m) => ({ default: m.StreamCosts })),
);

const NAV = [
  { id: "/", title: "Overview", icon: ChartMixed },
  { id: "/runtime", title: "Runtime", icon: Cpu },
  { id: "/health", title: "Health", icon: HeartPulse },
  { id: "/storage", title: "Storage", icon: Database },
  { id: "/stream-costs", title: "Stream costs", icon: Layers3Diagonal },
  { id: "/maintenance", title: "Maintenance", icon: Wrench },
];

const THEME_CYCLE: { next: Theme; icon: typeof Sun; hint: string }[] = [
  { next: "light", icon: Display, hint: "Theme: system" },
  { next: "dark", icon: Sun, hint: "Theme: light" },
  { next: "system", icon: Moon, hint: "Theme: dark" },
];

function ThemeButton() {
  const { theme, setTheme } = useAppTheme();
  const current = THEME_CYCLE[theme === "light" ? 1 : theme === "dark" ? 2 : 0];
  return (
    <Tooltip content={`${current.hint} — click to switch`}>
      <Button view="flat" onClick={() => setTheme(current.next)} aria-label="Switch theme">
        <Icon data={current.icon} />
      </Button>
    </Tooltip>
  );
}

function TopBar({ section }: { section: string }) {
  const qc = useQueryClient();
  const fetching = useIsFetching();

  return (
    <Flex className="topbar" alignItems="center" gap={3}>
      <span className="label-micro">{section}</span>
      <Flex grow />
      <ThemeButton />
      <Button
        view="outlined"
        loading={fetching > 0}
        onClick={() => qc.invalidateQueries()}
        aria-label="Refresh all data"
      >
        <Icon data={ArrowsRotateRight} />
        Refresh
      </Button>
    </Flex>
  );
}

export default function App() {
  const navigate = useNavigate();
  const { pathname } = useLocation();
  const [compact, setCompact] = useState(false);
  const info = useGetInfo({ query: { refetchInterval: 10_000 } });

  const menuItems = useMemo<AsideHeaderItem[]>(
    () =>
      NAV.map((item) => ({
        id: item.id,
        title: item.title,
        icon: item.icon,
        current: pathname === item.id,
        onItemClick: () => navigate(item.id),
      })),
    [pathname, navigate],
  );

  const section = NAV.find((item) => item.id === pathname)?.title ?? "Overview";

  const renderContent = useCallback(
    () => (
      <Flex direction="column" className="content">
        <TopBar section={section} />
        <Vitals />
        <div className="page">
          <Suspense fallback={<Loading />}>
            <Routes>
              <Route path="/" element={<Overview />} />
              <Route path="/runtime" element={<Runtime />} />
              <Route path="/health" element={<Health />} />
              <Route path="/storage" element={<Storage />} />
              <Route path="/stream-costs" element={<StreamCosts />} />
              <Route path="/maintenance" element={<Maintenance />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </Suspense>
        </div>
      </Flex>
    ),
    [section],
  );

  const renderFooter = useCallback(() => {
    if (compact || !info.data) return null;
    return (
      <Flex direction="column" spacing={{ px: 4, pb: 3 }} className="aside-footer">
        <Text variant="caption-2" color="hint" ellipsis>
          {info.data.version || "dev"}
        </Text>
        <Text variant="caption-2" color="hint" ellipsis>
          {info.data.os}/{info.data.arch}
        </Text>
      </Flex>
    );
  }, [compact, info.data]);

  return (
    <AsideHeader
      logo={{
        text: "oteldb",
        icon: GoFasterMark,
        iconSize: 24,
        onClick: () => navigate("/"),
      }}
      compact={compact}
      onChangeCompact={setCompact}
      headerDecoration
      menuItems={menuItems}
      renderContent={renderContent}
      renderFooter={renderFooter}
    />
  );
}
