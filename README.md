# bamdra-user-bind

`bamdra-user-bind` is the identity and profile binding plugin for the Bamdra OpenClaw memory suite.

It resolves channel-facing sender identifiers into a stable user boundary, stores user profile data locally, supports Feishu-oriented identity resolution, and exposes admin-safe tooling for querying and editing profile records.

## What It Does

- resolves `channel + sender.id` into a stable `userId`
- stores bindings and profiles in a local SQLite store
- exports human-readable backup files for inspection and recovery
- injects resolved identity into runtime context for downstream memory plugins
- blocks normal agents from reading other users' private data
- exposes separate admin tools for natural-language query, edit, merge, issue review, and resync workflows

## Open Source Contents

This repository already contains the actual plugin source code for the current open-source version.

- source entrypoint:
  [src/index.ts](/Users/wood/workspace/macmini-openclaw/openclaw-enhanced/bamdra-user-bind/src/index.ts)
- plugin manifest:
  [openclaw.plugin.json](/Users/wood/workspace/macmini-openclaw/openclaw-enhanced/bamdra-user-bind/openclaw.plugin.json)
- package metadata:
  [package.json](/Users/wood/workspace/macmini-openclaw/openclaw-enhanced/bamdra-user-bind/package.json)

The file count is intentionally small because this first public version is shipped as a compact plugin rather than a multi-package codebase.

## Current Storage Model

- runtime primary store:
  `~/.openclaw/data/bamdra-user-bind/profiles.sqlite`
- export directory:
  `~/.openclaw/data/bamdra-user-bind/exports/`

The runtime only queries the SQLite store. Export files exist for backup and manual inspection.

## Security Boundary

- normal agents can only read the current resolved user
- cross-user reads are denied by implementation, not by prompt wording alone
- admin actions are separated into dedicated tools
- audit records are written for admin reads, edits, merges, syncs, and rejected access attempts

## Relationship To Other Repositories

- required by:
  `bamdra-openclaw-memory`
- optional companion:
  `bamdra-memory-vector`

## Build

```bash
pnpm run bundle
```
