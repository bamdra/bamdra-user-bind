# Changelog

## v0.1.15 - 2026-03-27

### Added

- broader profile schema coverage for living user traits, including fields such as `birthDate`, `birthYear`, `age`, and `interests`
- pending-refine recovery so interrupted profile extraction work can be retried after startup instead of leaving stale pending markers behind

### Changed

- updated the profile skill guidance so profile extraction is driven by the skill and the structured schema rather than brittle hard-coded wording
- profile refinement now prefers configured fallback models when present and only falls back to the primary model when no separate extractor path exists

### Fixed

- expanded npm and runtime bootstrap so OpenClaw `v2026.3.23` writes correct `plugins.installs` metadata and resolves `~/.openclaw/...` paths against the active user home
- reduced the chance that profile extraction silently disappears after install because the plugin directory, skills, or standalone install metadata were only partially materialized

## v0.1.14 - 2026-03-24

### Added

- broader profile schema coverage for living user traits, including fields such as `birthDate`, `birthYear`, `age`, and `interests`
- pending-refine recovery so interrupted profile extraction work can be retried after startup instead of leaving stale pending markers behind

### Changed

- updated the profile skill guidance so profile extraction is driven by the skill and the structured schema rather than brittle hard-coded wording
- profile refinement now prefers configured fallback models when present and only falls back to the primary model when no separate extractor path exists

### Fixed

- expanded npm and runtime bootstrap so OpenClaw `v2026.3.23` writes correct `plugins.installs` metadata and resolves `~/.openclaw/...` paths against the active user home
- reduced the chance that profile extraction silently disappears after install because the plugin directory, skills, or standalone install metadata were only partially materialized

## v0.1.13 - 2026-03-23

### Added

- semantic profile update operations so stable user traits can be `replace`, `append`, or `remove` instead of always overwriting the old field
- provisional profile persistence and later merge, allowing durable user preferences to be captured before stable binding repair finishes
- broader channel-scoped identity support across Feishu, Telegram, WhatsApp, Discord, Google Chat, Slack, Mattermost, Signal, iMessage, and Microsoft Teams

### Changed

- clarified the profile Markdown mirror so frontmatter remains the machine-readable source and the body becomes a synchronized human-readable summary
- improved profile skill guidance so agents quietly read the bound profile first, repair identity when possible, and write stable traits back through the profile tools

### Fixed

- stopped persisting misleading synthetic user IDs like `feishu:main`
- removed hard-coded default nicknames, personality hints, and timezones in favor of lightweight defaults and environment-based timezone fallback
- hardened profile sync and message-capture paths so later binding repair can merge provisional data into the stable user profile
