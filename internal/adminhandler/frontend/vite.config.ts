import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The built SPA is embedded into the Go binary (internal/adminhandler/ui.go)
// and served from the admin API's origin, so assets use relative paths.
export default defineConfig({
  plugins: [react()],
  base: "./",
  build: {
    outDir: "dist",
    emptyOutDir: true,
    // A single instance is small; inline nothing huge but keep the bundle lean.
    chunkSizeWarningLimit: 1200,
  },
  server: {
    port: 5273,
    // During `bun run dev`, proxy API calls to a locally running oteldb admin.
    proxy: {
      "/api": "http://127.0.0.1:8090",
    },
  },
});
