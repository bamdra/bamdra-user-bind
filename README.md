# bamdra-user-bind

The identity and living profile layer for the Bamdra suite.

It can run independently, and it is also auto-provisioned by `bamdra-openclaw-memory`.

[中文文档](./README.zh-CN.md)

## What it does

`bamdra-user-bind` turns raw channel sender IDs into a stable user boundary.

It also becomes the user's evolving profile layer, including:

- preferred address
- timezone
- tone preferences
- role
- long-lived user notes

## Why it matters

Without an identity layer:

- the same person can fragment across channels or sessions
- memory can attach to the wrong boundary
- personalization becomes fragile

With it:

- user-aware memory becomes stable
- personalization survives new sessions
- the assistant can gradually adapt to the user's style and working habits

## Storage model

- primary store:
  `~/.openclaw/data/bamdra-user-bind/profiles.sqlite`
- editable Markdown mirrors:
  `~/.openclaw/data/bamdra-user-bind/profiles/private/{userId}.md`
- export directory:
  `~/.openclaw/data/bamdra-user-bind/exports/`

The SQLite store is the controlled source of truth.

The Markdown mirror is for humans, so profiles stay editable like a living per-user guide instead of becoming a hidden black box.

## Best practice

- keep SQLite local
- keep profile mirrors private
- let humans edit the mirror gradually
- use admin tools only for audit, merge, repair, and maintenance

## What it unlocks

With `bamdra-openclaw-memory`:

- memory becomes user-aware instead of session-only

With `bamdra-memory-vector`:

- private notes stay private while still influencing local recall

## Repository

- [GitHub organization](https://github.com/bamdra)
- [Repository](https://github.com/bamdra/bamdra-user-bind)
