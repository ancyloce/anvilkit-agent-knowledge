// The production build: two ESM entries under dist/ (the service and the
// DEVELOPMENT_ONLY local-check owner entry point) with the generated contract
// consumers (@anvilkit/generated-clients, TypeScript sources of the pinned
// contracts commit) compiled in and every other package left external, so
// the runtime resolves the locked node_modules of this package and nothing
// outside it. Every package the bundle leaves external must be a production
// dependency of this package, which esbuild's metafile is checked for here.
import { readFileSync } from "node:fs";
import { build } from "esbuild";

const { dependencies = {} } = JSON.parse(readFileSync(new URL("./package.json", import.meta.url), "utf8"));

const result = await build({
	entryPoints: ["src/main.ts", "src/localcheck.ts", "src/storemigrate.ts", "src/memoryctl.ts"],
	bundle: true,
	platform: "node",
	format: "esm",
	target: "node24",
	outdir: "dist",
	sourcemap: false,
	logLevel: "warning",
	metafile: true,
	plugins: [
		{
			name: "externals",
			setup(b) {
				b.onResolve({ filter: /^[^./#]/ }, (args) =>
					args.path.startsWith("@anvilkit/") ? undefined : { path: args.path, external: true },
				);
			},
		},
	],
});

const packageOf = (specifier) =>
	specifier
		.split("/")
		.slice(0, specifier.startsWith("@") ? 2 : 1)
		.join("/");
const externals = new Set(
	Object.values(result.metafile.outputs)
		.flatMap((o) => o.imports)
		.filter((i) => i.external && !i.path.startsWith("node:"))
		.map((i) => packageOf(i.path)),
);
const undeclared = [...externals].filter((p) => !(p in dependencies)).sort();
if (undeclared.length > 0) {
	console.error(`dist imports packages that are not production dependencies: ${undeclared.join(", ")}`);
	process.exit(1);
}
