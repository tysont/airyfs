import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Exec tests attach a real Container (warm ~2-10s, cold ~30s), so give
    // each test and hook generous headroom.
    testTimeout: 120_000,
    hookTimeout: 120_000,
    // One volume/Container has a single execution slot; keep the file
    // sequential so concurrent describe blocks don't fight over it.
    fileParallelism: false,
    pool: "forks",
  },
});
