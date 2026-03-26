---
name: bamdra-user-bind-admin
description: Use the admin-safe natural-language tools to inspect, fix, merge, and sync user bindings and profiles.
---

# Bamdra User Bind Admin

Use this skill only on an authorized admin agent.

Its purpose is operational: inspect user bindings, repair incorrect profile fields, merge duplicate users, and check sync issues without exposing unrestricted bulk access.

## Allowed Jobs

- query a specific user profile or binding by `userId`
- correct name, nickname, gender, birthday, age, role, timezone, preferences, personality, interests, or other profile fields
- merge duplicate user records
- inspect sync failures and identity resolution issues
- request a resync for a known user

## Tooling

Use the admin tools in natural language:

- `user_bind_admin_query`
- `user_bind_admin_edit`
- `user_bind_admin_merge`
- `user_bind_admin_list_issues`
- `user_bind_admin_sync`

## Good Requests

- “查询 user:u_123 的画像和绑定关系”
- “把 user:u_123 的姓名改成李明，时区改成 Asia/Tokyo”
- “把 user:u_123 的兴趣改成摄影和骑行”
- “把 user:u_123 的默认称呼改成小李”
- “合并 user:u_old 到 user:u_new”
- “列出最近的绑定失败问题”

## Safety Rules

- do not perform blind bulk edits
- prefer specific target users over fuzzy descriptions
- if a request is ambiguous, narrow it before making changes
- remember that every admin action is auditable
- do not expose unrelated users when answering a narrow admin query
- do not use admin tools as a shortcut for answering the current user's own profile or address preference
- if the issue is that the current session cannot resolve the user, repair the binding first, then let the normal self-profile flow continue
