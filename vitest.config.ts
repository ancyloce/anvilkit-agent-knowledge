import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		include: ["test/**/*.test.ts"],
		testTimeout: 180_000,
		hookTimeout: 180_000,
		// One disposable PostgreSQL per file; the process-level lifecycle
		// scenario binds loopback listeners.
		fileParallelism: false,
		server: {
			deps: {
				inline: [/@anvilkit\/generated-clients/],
			},
		},
	},
});
