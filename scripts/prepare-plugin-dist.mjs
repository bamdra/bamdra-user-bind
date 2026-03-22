import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const repoRoot = resolve(import.meta.dirname, "..");
const distDir = resolve(repoRoot, "dist");

mkdirSync(distDir, { recursive: true });

const sourcePackage = JSON.parse(readFileSync(resolve(repoRoot, "package.json"), "utf8"));
const sourceManifest = JSON.parse(readFileSync(resolve(repoRoot, "openclaw.plugin.json"), "utf8"));

const distPackage = {
  ...sourcePackage,
  type: "commonjs",
  main: "./index.js",
  openclaw: {
    ...(sourcePackage.openclaw || {}),
    manifest: "./openclaw.plugin.json",
    extensions: ["./index.js"],
  },
};

const distManifest = {
  ...sourceManifest,
  version: sourcePackage.version,
  main: "./index.js",
};

writeFileSync(resolve(distDir, "package.json"), `${JSON.stringify(distPackage, null, 2)}\n`);
writeFileSync(resolve(distDir, "openclaw.plugin.json"), `${JSON.stringify(distManifest, null, 2)}\n`);
