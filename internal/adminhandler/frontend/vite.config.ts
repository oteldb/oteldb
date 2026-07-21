import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { writeFileSync } from "node:fs";

// The built SPA (dist/) is not committed; only a dist/.gitkeep placeholder is,
// so the Go go:embed directive compiles on a fresh checkout. emptyOutDir wipes
// that placeholder on every build, so recreate it once the bundle is written.
function keepPlaceholder() {
  return {
    name: "keep-dist-placeholder",
    closeBundle() {
      writeFileSync("dist/.gitkeep", "");
    },
  };
}

// The built SPA is embedded into the Go binary (internal/adminhandler/ui.go)
// and served from the admin API's origin, so assets use relative paths.
export default defineConfig({
  plugins: [react(), keepPlaceholder()],
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
