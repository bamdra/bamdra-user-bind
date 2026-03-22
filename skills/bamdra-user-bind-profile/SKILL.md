---
name: bamdra-user-bind-profile
description: Use the current bound user profile as the primary personalization layer for tone, address, timezone, and stable preferences.
---

# Bamdra User Bind Profile

Treat `bamdra-user-bind` as the current user's editable personalization layer.

This skill is about the current bound user only. It should cover most of what a per-user `USER.md` would normally do, without leaking across users.

## What To Use It For

- preferred form of address
- timezone-aware responses
- stable tone and formatting preferences
- role-aware defaults
- long-lived collaboration preferences
- private notes the user intentionally keeps in their profile

## Profile Source Of Truth

The runtime profile comes from `bamdra-user-bind`.

Humans can edit the Markdown mirror for the current user, and the plugin will sync that into the controlled store. Treat the bound profile as more authoritative than guesswork.

Keep per-user address preferences in the bound profile instead of `USER.md`. `USER.md` should stay minimal and only carry environment facts that are not identity-specific.

## Behavior Rules

- personalize naturally when the stored profile clearly helps
- use the stored nickname if the user has not asked for a different form of address in the current turn
- respect the stored timezone for scheduling, reminders, dates, and time-sensitive explanations
- prefer the stored tone/style preferences when shaping responses
- if the profile contains a preferred address, treat it as the default greeting and do not duplicate it in workspace-level `USER.md`
- if the current turn conflicts with the stored profile, follow the current turn
- do not invent profile traits that are not present

## Privacy Rules

- do not try to infer or mention other users' profiles
- do not describe the private profile store unless the user asks
- do not treat profile storage as a global contact directory
- do not ask for profile data that already exists in the bound profile unless it is stale or clearly insufficient

## Updating Profile Information

When the user clearly provides a stable preference or asks to remember how to work with them, it is appropriate to update their own profile.

Good examples:

- “以后叫我老板”
- “我在 Asia/Shanghai 时区”
- “我偏好幽默一点，但别太浮夸”
- “我更喜欢先给结论，再展开”

Do not update the profile for transient moods, one-off formatting requests, or unstable short-term details.

## Storage Boundary

Treat profile-style facts as private-by-default.

These should stay in the current user's profile or user-scoped memory, not `shared` memory:

- 称呼方式
- 时区
- 对话风格偏好
- 角色和职责
- 宠物、家庭、个人背景
- 当前主要工作和长期个人目标

Only treat a statement as shared if the user clearly frames it as a team rule, a reusable public fact, or something that should apply beyond themselves.
