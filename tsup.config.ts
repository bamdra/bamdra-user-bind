import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["cjs"],
  clean: false,
  bundle: true,
  shims: true,
  target: "node22",
  splitting: false,
  noExternal: [/.*/],
  external: [
    "node:crypto",
    "node:fs",
    "node:fs/promises",
    "node:os",
    "node:path",
    "node:sqlite"
  ],
  outExtension() {
    return { js: ".js" };
  },
  esbuildOptions(options) {
    options.platform = "node";
    options.target = "node22";
  }
});
