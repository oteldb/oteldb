import { defineConfig } from "orval";

// Generates a fully typed TanStack Query client from the admin OpenAPI spec.
// The spec is the source of truth (internal/adminhandler is served by the ogen
// server generated from the same _oas/admin.yml).
export default defineConfig({
  admin: {
    input: {
      target: "../../../_oas/admin.yml",
    },
    output: {
      mode: "split",
      target: "./src/api/admin.ts",
      schemas: "./src/api/model",
      client: "react-query",
      // Same-origin: the admin API is served alongside the UI, so paths stay
      // relative (the spec's absolute server URL is ignored) via the mutator.
      baseUrl: "",
      clean: true,
      prettier: false,
      override: {
        query: {
          useQuery: true,
        },
        mutator: {
          path: "./src/lib/fetcher.ts",
          name: "customFetch",
        },
      },
    },
  },
});
