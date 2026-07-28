import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from "react";
import { ThemeProvider, type Theme } from "@gravity-ui/uikit";

const STORAGE_KEY = "oteldb.admin.theme";
const THEMES: Theme[] = ["system", "light", "dark"];

function readStoredTheme(): Theme {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    return THEMES.includes(raw as Theme) ? (raw as Theme) : "system";
  } catch {
    // Private mode or blocked storage: fall back to the system preference.
    return "system";
  }
}

interface AppThemeContext {
  theme: Theme;
  setTheme: (theme: Theme) => void;
}

const Ctx = createContext<AppThemeContext>({ theme: "system", setTheme: () => {} });

export function useAppTheme(): AppThemeContext {
  return useContext(Ctx);
}

/** Wraps the app in the Gravity UI ThemeProvider and persists the chosen theme. */
export function AppThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setThemeState] = useState<Theme>(readStoredTheme);

  const setTheme = useCallback((next: Theme) => {
    setThemeState(next);
    try {
      window.localStorage.setItem(STORAGE_KEY, next);
    } catch {
      // Persisting is best-effort.
    }
  }, []);

  const value = useMemo(() => ({ theme, setTheme }), [theme, setTheme]);

  return (
    <Ctx.Provider value={value}>
      <ThemeProvider theme={theme}>{children}</ThemeProvider>
    </Ctx.Provider>
  );
}
