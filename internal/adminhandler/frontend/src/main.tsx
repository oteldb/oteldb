import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { HashRouter } from "react-router-dom";
import { Toaster, ToasterComponent, ToasterProvider } from "@gravity-ui/uikit";
import App from "./App";
import { AppThemeProvider } from "./lib/theme";

// uikit's fonts.css @imports Inter from Google Fonts. This console ships inside
// the oteldb binary and must render the same on a host with no internet, so the
// type stack is set from system faces in app.css instead.
import "@gravity-ui/uikit/styles/styles.css";
import "./brand.css";
import "./app.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: 2_000,
      refetchOnWindowFocus: false,
    },
  },
});

const toaster = new Toaster();

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <AppThemeProvider>
      <ToasterProvider toaster={toaster}>
        <QueryClientProvider client={queryClient}>
          <HashRouter>
            <App />
          </HashRouter>
        </QueryClientProvider>
        <ToasterComponent />
      </ToasterProvider>
    </AppThemeProvider>
  </StrictMode>,
);
