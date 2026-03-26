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

- before replying, quietly fetch the current bound profile and use it as a soft personalization hint rather than announcing the lookup
- personalize naturally when the stored profile clearly helps
- use the stored nickname if the user has not asked for a different form of address in the current turn
- respect the stored timezone for scheduling, reminders, dates, and time-sensitive explanations
- prefer the stored tone/style preferences when shaping responses
- if the profile contains a preferred address, treat it as the default greeting and do not duplicate it in workspace-level `USER.md`
- if the current turn conflicts with the stored profile, follow the current turn
- do not invent profile traits that are not present
- if the current open id fails to match a bound user, first try the identity refresh and channel resolution path to recover the real uid before treating the profile as missing
- remember that one real user may correspond to multiple open ids across apps or accounts; prefer repairing the binding over creating parallel personas
- if the identity is recovered but the profile is sparse, create only a lightweight default profile and continue naturally
- when a lightweight default profile is in use, ask at most one or two brief, human questions such as how to address the user or what communication style they prefer
- phrase onboarding naturally, for example as a first-time acquaintance check, not as a database form or a profile interrogation
- once the user gives a stable preference like preferred address or collaboration style, update their bound profile immediately instead of leaving it in `USER.md`

## Tool Actions

Use the profile tools as an action loop, not as a passive reference.

1. Before replying, call `bamdra_user_bind_get_my_profile`.
2. If current identity looks missing or stale, call `bamdra_user_bind_refresh_my_binding` first, then read the profile again.
3. If the user clearly reveals stable profile information in the current turn, first do a semantic interpretation of what belongs in the user's profile, then call `bamdra_user_bind_update_my_profile` in the same turn.
4. After a successful update, reply naturally as if you have already absorbed the preference. Do not say that you "might" remember it later.

When the user reveals stable profile information, do not stop at "我记住了" in prose. Actually write it back through the tool.

If you just asked a profile-collection question and the user replies briefly, that short answer still counts as profile information. Examples: "叫我小李就行", "直接点", "先给结论". Do not wait for a longer sentence before calling `bamdra_user_bind_update_my_profile`.

Do not surface internal profile-write failures in the user-facing reply unless the user explicitly asks about the profile system itself. Avoid lines like "系统画像更新遇到了一点技术问题". If the write path needs repair in the background, keep the reply natural and continue serving the user.

## Semantic Extraction Rule

Do not rely on phrase matching or a tiny list of trigger sentences.

Use model judgment to read the user's natural language and decide whether the message contains stable profile traits that should persist beyond the current turn.

Think in terms of profile slots, not keywords:

- the user's name or self-identification
- how the user prefers to be addressed
- gender when the user explicitly provides it
- birthday, birth date, birth year, age, or other durable demographic facts when explicitly provided
- how the user prefers answers to be structured
- tone and communication style
- long-lived interests, hobbies, or personal background that help future collaboration
- role, responsibility, or identity in the collaboration
- timezone or durable location context
- durable dislikes, boundaries, or collaboration constraints
- long-lived private notes that help future cooperation

The same user intent may appear in many surface forms. Treat all of these as equivalent if they carry the same stable meaning:

- direct statements
- soft suggestions
- casual side remarks
- corrections to how you addressed them
- "by the way" comments
- complaints about prior reply style
- self-descriptions embedded inside a larger request

Extract semantically. Do not wait for the user to say exactly "remember this" or "update my profile".

If a message contains both a task request and profile information, do both:

- first update the profile with the stable traits you inferred
- then continue handling the user's actual task

Field mapping for `bamdra_user_bind_update_my_profile`:

- explicit self-identification -> `name`
- preferred form of address -> `nickname`
- explicit gender statement -> `gender`
- birthday or birth date -> `birthDate`
- birth year or birth month/year -> `birthYear`
- explicit age -> `age`
- communication style or formatting preference -> `preferences`
- personality or tone preference -> `personality`
- durable interests, hobbies, or recurring passions -> `interests`
- role or identity in collaboration -> `role`
- timezone -> `timezone`
- durable private notes worth remembering -> `notes`

When mapping, summarize the stable meaning instead of copying noisy raw text when helpful.

Examples of semantic normalization:

- "以后别整那么官腔" -> `personality` or `preferences`, not raw transcript dumping
- "我比较喜欢你先说判断，再展开原因" -> `preferences`
- "我叫李明" -> `name`
- "我生日是 1994-08-12" -> `birthDate`
- "我今年 31 岁" -> `age`
- "我平时喜欢骑行和摄影" -> `interests`
- "我是这个项目最后拍板的人" -> `role`
- "别老叫我老师，直接叫我名字就行" -> `nickname` or `notes`, depending on what is clearest
- "我不喜欢太长铺垫" -> `preferences` or `notes`

Examples:

- "我叫李明" -> call `bamdra_user_bind_update_my_profile` with `name: "李明"`
- "以后叫我小李" -> call `bamdra_user_bind_update_my_profile` with `nickname: "小李"`
- "我是女性" -> call `bamdra_user_bind_update_my_profile` with `gender: "女性"`
- "我生日是 1994-08-12" -> call `bamdra_user_bind_update_my_profile` with `birthDate: "1994-08-12"`
- "我今年 31 岁" -> call `bamdra_user_bind_update_my_profile` with `age: "31"`
- "你先给结论，再展开" -> call `bamdra_user_bind_update_my_profile` with `preferences: "偏好先给结论，再展开"`
- "说话直接一点，但别太冲" -> call `bamdra_user_bind_update_my_profile` with `personality: "偏好直接、克制、不生硬"`
- "我喜欢骑行和摄影" -> call `bamdra_user_bind_update_my_profile` with `interests: "骑行；摄影"`
- "我常驻东京" -> call `bamdra_user_bind_update_my_profile` with `timezone: "Asia/Tokyo"`
- "我是这个项目的负责人" -> call `bamdra_user_bind_update_my_profile` with `role: "项目负责人"`

If one user message contains both service intent and stable profile facts, do both in the same turn:

- first update the profile
- then continue handling the actual request

## Privacy Rules

- do not try to infer or mention other users' profiles
- do not describe the private profile store unless the user asks
- do not treat profile storage as a global contact directory
- do not ask for profile data that already exists in the bound profile unless it is stale or clearly insufficient
- do not force explicit profile talk when the information can stay implicit in the response style

## Updating Profile Information

When the user clearly provides a stable preference or asks to remember how to work with them, it is appropriate to update their own profile.

Good examples:

- “以后叫我小李”
- “我在 Asia/Tokyo 时区”
- “我偏好幽默一点，但别太浮夸”
- “我更喜欢先给结论，再展开”
- “应该叫我名字，不用太正式”
- “第一次配合的话，你可以先简短一点”
- “别太像客服，直接一点”
- “我是这个项目的负责人”
- “我不喜欢很长的铺垫”

Do not update the profile for transient moods, one-off formatting requests, or unstable short-term details.

Use this stability test:

- if it is likely to still help in later conversations, it probably belongs in the profile
- if it only matters for this single reply, it probably does not

## Storage Boundary

Treat profile-style facts as private-by-default.

These should stay in the current user's profile or user-scoped memory, not `shared` memory:

- 姓名
- 性别
- 生日 / 出生年月 / 年龄
- 称呼方式
- 时区
- 对话风格偏好
- 兴趣爱好
- 角色和职责
- 宠物、家庭、个人背景
- 当前主要工作和长期个人目标

Only treat a statement as shared if the user clearly frames it as a team rule, a reusable public fact, or something that should apply beyond themselves.
