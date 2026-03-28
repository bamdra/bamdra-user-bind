import test from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, cpSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { execFileSync } from "node:child_process";

test("prepare-plugin-dist copies skills into dist", () => {
  const fixtureRoot = mkdtempSync(join(tmpdir(), "bamdra-user-bind-dist-"));
  const repoCopy = join(fixtureRoot, "repo");
  const sourceRoot = resolve(import.meta.dirname, "..");

  cpSync(sourceRoot, repoCopy, {
    recursive: true,
    filter(source) {
      return !source.includes(`${resolve(sourceRoot, "artifacts")}`) && !source.includes(`${resolve(sourceRoot, "node_modules")}`);
    },
  });

  rmSync(join(repoCopy, "dist", "skills"), { recursive: true, force: true });
  execFileSync(process.execPath, [join(repoCopy, "scripts", "prepare-plugin-dist.mjs")], {
    cwd: repoCopy,
  });

  assert.equal(existsSync(join(repoCopy, "dist", "skills", "bamdra-user-bind-profile", "SKILL.md")), true);
  assert.equal(existsSync(join(repoCopy, "dist", "skills", "bamdra-user-bind-admin", "SKILL.md")), true);
});
