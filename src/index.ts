import { createHash } from "node:crypto";
import { cpSync, existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";

const PLUGIN_ID = "bamdra-user-bind";
const GLOBAL_API_KEY = "__OPENCLAW_BAMDRA_USER_BIND__";
const PROFILE_SKILL_ID = "bamdra-user-bind-profile";
const ADMIN_SKILL_ID = "bamdra-user-bind-admin";
const SELF_TOOL_NAMES = [
  "bamdra_user_bind_get_my_profile",
  "bamdra_user_bind_update_my_profile",
  "bamdra_user_bind_refresh_my_binding",
] as const;
const ADMIN_TOOL_NAMES = [
  "bamdra_user_bind_admin_query",
  "bamdra_user_bind_admin_edit",
  "bamdra_user_bind_admin_merge",
  "bamdra_user_bind_admin_list_issues",
  "bamdra_user_bind_admin_sync",
] as const;
const TABLES = {
  profiles: "bamdra_user_bind_profiles",
  bindings: "bamdra_user_bind_bindings",
  issues: "bamdra_user_bind_issues",
  audits: "bamdra_user_bind_audits",
} as const;
const REQUIRED_FEISHU_IDENTITY_SCOPES = [
  "contact:user.employee_id:readonly",
  "contact:user.base:readonly",
] as const;

export interface UserProfile {
  userId: string;
  name: string | null;
  gender: string | null;
  email: string | null;
  avatar: string | null;
  nickname: string | null;
  preferences: string | null;
  personality: string | null;
  role: string | null;
  timezone: string | null;
  notes: string | null;
  visibility: "private" | "shared";
  source: string;
  updatedAt: string;
}

export interface ResolvedIdentity {
  sessionId: string;
  userId: string;
  channelType: string;
  senderOpenId: string | null;
  senderName: string | null;
  source: string;
  resolvedAt: string;
  profile: UserProfile;
}

export interface UserBindConfig {
  enabled: boolean;
  localStorePath: string;
  exportPath: string;
  profileMarkdownRoot: string;
  cacheTtlMs: number;
  adminAgents: string[];
}

interface UserBindToolResult {
  content: Array<{ type: "text"; text: string }>;
}

interface ToolDefinition<TParams> {
  name: string;
  description: string;
  parameters: Record<string, unknown>;
  execute(invocationId: string, params: TParams): Promise<UserBindToolResult>;
}

interface HookApi {
  registerHook?: (
    events: string | string[],
    handler: (event: unknown) => unknown | Promise<unknown>,
    opts?: { name?: string; description?: string; priority?: number },
  ) => void;
  on?: (
    hookName: string,
    handler: (event: unknown, context: unknown) => unknown | Promise<unknown>,
    opts?: { priority?: number },
  ) => void;
  registerTool?<TParams>(definition: ToolDefinition<TParams>): void;
  callTool?<TParams>(name: string, params: TParams): Promise<unknown>;
  invokeTool?<TParams>(name: string, params: TParams): Promise<unknown>;
  pluginConfig?: Partial<UserBindConfig>;
  config?: Partial<UserBindConfig>;
  plugin?: { config?: Partial<UserBindConfig> };
}

interface AuditRecord {
  id: string;
  ts: string;
  agentId: string | null;
  requesterUserId: string | null;
  toolName: string;
  instruction: string;
  resolvedAction: string;
  targetUserIds: string[];
  decision: string;
  changedFields: string[];
}

interface ParsedMarkdownProfile {
  profilePatch: Partial<UserProfile>;
  notes: string | null;
}

interface FeishuUserResolution {
  userId: string;
  source: string;
  profilePatch: Partial<UserProfile>;
}

interface FeishuScopeStatus {
  scopes: string[];
  missingIdentityScopes: string[];
  hasDocumentAccess: boolean;
}

interface FeishuAccountCredentials {
  accountId: string;
  appId: string;
  appSecret: string;
  domain: string;
}

function logUserBindEvent(event: string, details: Record<string, unknown> = {}): void {
  try {
    console.info("[bamdra-user-bind]", event, JSON.stringify(details));
  } catch {
    console.info("[bamdra-user-bind]", event);
  }
}

class UserBindStore {
  readonly db: DatabaseSync;
  private readonly markdownSyncing = new Set<string>();

  constructor(
    private readonly dbPath: string,
    private readonly exportPath: string,
    private readonly profileMarkdownRoot: string,
  ) {
    mkdirSync(dirname(dbPath), { recursive: true });
    mkdirSync(exportPath, { recursive: true });
    mkdirSync(profileMarkdownRoot, { recursive: true });
    this.db = new DatabaseSync(dbPath);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS ${TABLES.profiles} (
        user_id TEXT PRIMARY KEY,
        name TEXT,
        gender TEXT,
        email TEXT,
        avatar TEXT,
        nickname TEXT,
        preferences TEXT,
        personality TEXT,
        role TEXT,
        timezone TEXT,
        notes TEXT,
        visibility TEXT NOT NULL DEFAULT 'private',
        source TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS ${TABLES.bindings} (
        binding_id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        channel_type TEXT NOT NULL,
        open_id TEXT,
        external_user_id TEXT,
        union_id TEXT,
        source TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(channel_type, open_id)
      );
      CREATE TABLE IF NOT EXISTS ${TABLES.issues} (
        id TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        user_id TEXT,
        details TEXT NOT NULL,
        status TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS ${TABLES.audits} (
        id TEXT PRIMARY KEY,
        ts TEXT NOT NULL,
        agent_id TEXT,
        requester_user_id TEXT,
        tool_name TEXT NOT NULL,
        instruction TEXT NOT NULL,
        resolved_action TEXT NOT NULL,
        target_user_ids TEXT NOT NULL,
        decision TEXT NOT NULL,
        changed_fields TEXT NOT NULL
      );
    `);
    this.ensureColumn(TABLES.profiles, "timezone", "TEXT");
    this.ensureColumn(TABLES.profiles, "notes", "TEXT");
  }

  close(): void {
    this.db.close();
  }

  getProfile(userId: string): UserProfile | null {
    this.syncMarkdownToStore(userId);
    return this.getProfileFromDatabase(userId);
  }

  findBinding(channelType: string, openId: string | null): { userId: string; source: string } | null {
    if (!openId) {
      return null;
    }
    const row = this.db.prepare(`
      SELECT user_id, source FROM ${TABLES.bindings}
      WHERE channel_type = ? AND open_id = ?
    `).get(channelType, openId) as { user_id: string; source: string } | undefined;
    if (!row) {
      return null;
    }
    return {
      userId: row.user_id,
      source: row.source,
    };
  }

  listIssues(): Array<Record<string, unknown>> {
    return this.db.prepare(`
      SELECT id, kind, user_id, details, status, updated_at
      FROM ${TABLES.issues}
      ORDER BY updated_at DESC
    `).all() as Array<Record<string, unknown>>;
  }

  recordIssue(kind: string, details: string, userId: string | null = null): void {
    const now = new Date().toISOString();
    const id = hashId(`${kind}:${userId ?? "none"}:${details}`);
    this.db.prepare(`
      INSERT INTO ${TABLES.issues} (id, kind, user_id, details, status, updated_at)
      VALUES (?, ?, ?, ?, 'open', ?)
      ON CONFLICT(id) DO UPDATE SET details = excluded.details, updated_at = excluded.updated_at
    `).run(id, kind, userId, details, now);
  }

  writeAudit(record: AuditRecord): void {
    this.db.prepare(`
      INSERT INTO ${TABLES.audits} (id, ts, agent_id, requester_user_id, tool_name, instruction, resolved_action, target_user_ids, decision, changed_fields)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      record.id,
      record.ts,
      record.agentId,
      record.requesterUserId,
      record.toolName,
      record.instruction,
      record.resolvedAction,
      JSON.stringify(record.targetUserIds),
      record.decision,
      JSON.stringify(record.changedFields),
    );
  }

  upsertIdentity(args: {
    userId: string;
    channelType: string;
    openId: string | null;
    source: string;
    profilePatch: Partial<UserProfile>;
  }): UserProfile {
    if (!this.markdownSyncing.has(args.userId)) {
      this.syncMarkdownToStore(args.userId);
    }
    const now = new Date().toISOString();
    const current = this.getProfileFromDatabase(args.userId);
    const next: UserProfile = {
      userId: args.userId,
      name: args.profilePatch.name ?? current?.name ?? null,
      gender: args.profilePatch.gender ?? current?.gender ?? null,
      email: args.profilePatch.email ?? current?.email ?? null,
      avatar: args.profilePatch.avatar ?? current?.avatar ?? null,
      nickname: args.profilePatch.nickname ?? current?.nickname ?? "老板",
      preferences: args.profilePatch.preferences ?? current?.preferences ?? "幽默诙谐的对话风格，但是不过分",
      personality: args.profilePatch.personality ?? current?.personality ?? null,
      role: args.profilePatch.role ?? current?.role ?? null,
      timezone: args.profilePatch.timezone ?? current?.timezone ?? "Asia/Shanghai",
      notes: args.profilePatch.notes ?? current?.notes ?? defaultProfileNotes(),
      visibility: args.profilePatch.visibility ?? current?.visibility ?? "private",
      source: args.source,
      updatedAt: now,
    };

    this.db.prepare(`
      INSERT INTO ${TABLES.profiles} (user_id, name, gender, email, avatar, nickname, preferences, personality, role, timezone, notes, visibility, source, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(user_id) DO UPDATE SET
        name = excluded.name,
        gender = excluded.gender,
        email = excluded.email,
        avatar = excluded.avatar,
        nickname = excluded.nickname,
        preferences = excluded.preferences,
        personality = excluded.personality,
        role = excluded.role,
        timezone = excluded.timezone,
        notes = excluded.notes,
        visibility = excluded.visibility,
        source = excluded.source,
        updated_at = excluded.updated_at
    `).run(
      next.userId,
      next.name,
      next.gender,
      next.email,
      next.avatar,
      next.nickname,
      next.preferences,
      next.personality,
      next.role,
      next.timezone,
      next.notes,
      next.visibility,
      next.source,
      next.updatedAt,
    );

    if (args.channelType !== "manual" || args.openId) {
      const bindingId = hashId(`${args.channelType}:${args.openId ?? args.userId}`);
      this.db.prepare(`
        INSERT INTO ${TABLES.bindings} (binding_id, user_id, channel_type, open_id, external_user_id, union_id, source, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(binding_id) DO UPDATE SET
          user_id = excluded.user_id,
          channel_type = excluded.channel_type,
          open_id = excluded.open_id,
          source = excluded.source,
          updated_at = excluded.updated_at
      `).run(
        bindingId,
        args.userId,
        args.channelType,
        args.openId,
        args.userId,
        null,
        args.source,
        now,
      );
    }

    this.writeProfileMarkdown(next);
    this.writeExports();
    return next;
  }

  updateProfile(userId: string, patch: Partial<UserProfile>): UserProfile {
    const current = this.getProfile(userId);
    if (!current) {
      throw new Error(`Unknown user ${userId}`);
    }
    return this.upsertIdentity({
      userId,
      channelType: "manual",
      openId: null,
      source: patch.source ?? "manual",
      profilePatch: {
        ...current,
        ...patch,
      },
    });
  }

  mergeUsers(fromUserId: string, intoUserId: string): UserProfile {
    const from = this.getProfile(fromUserId);
    const into = this.getProfile(intoUserId);
    if (!from || !into) {
      throw new Error("Both source and target users must exist before merge");
    }
    const merged = this.upsertIdentity({
      userId: intoUserId,
      channelType: "manual",
      openId: null,
      source: "admin-merge",
      profilePatch: {
        name: into.name ?? from.name,
        gender: into.gender ?? from.gender,
        email: into.email ?? from.email,
        avatar: into.avatar ?? from.avatar,
        nickname: into.nickname ?? from.nickname,
        preferences: into.preferences ?? from.preferences,
        personality: into.personality ?? from.personality,
        role: into.role ?? from.role,
        timezone: into.timezone ?? from.timezone,
        notes: joinNotes(into.notes, from.notes),
        visibility: into.visibility,
      },
    });
    this.db.prepare(`UPDATE ${TABLES.bindings} SET user_id = ?, updated_at = ? WHERE user_id = ?`).run(
      intoUserId,
      new Date().toISOString(),
      fromUserId,
    );
    this.db.prepare(`DELETE FROM ${TABLES.profiles} WHERE user_id = ?`).run(fromUserId);
    this.writeExports();
    return merged;
  }

  private getProfileFromDatabase(userId: string): UserProfile | null {
    const row = this.db.prepare(`
      SELECT user_id, name, gender, email, avatar, nickname, preferences, personality, role, timezone, notes, visibility, source, updated_at
      FROM ${TABLES.profiles} WHERE user_id = ?
    `).get(userId) as Record<string, unknown> | undefined;
    return row ? mapProfileRow(row) : null;
  }

  private profileMarkdownPath(userId: string): string {
    return join(this.profileMarkdownRoot, `${sanitizeFilename(userId)}.md`);
  }

  private syncMarkdownToStore(userId: string): void {
    if (this.markdownSyncing.has(userId)) {
      return;
    }
    this.markdownSyncing.add(userId);
    try {
    const markdownPath = this.profileMarkdownPath(userId);
    if (!existsSync(markdownPath)) {
      const current = this.getProfileFromDatabase(userId);
      if (current) {
        this.writeProfileMarkdown(current);
      }
      return;
    }
    const current = this.getProfileFromDatabase(userId);
    const parsed = parseProfileMarkdown(readFileSync(markdownPath, "utf8"));
    const markdownMtime = statSync(markdownPath).mtime.toISOString();
    const dbTime = current?.updatedAt ?? new Date(0).toISOString();
    const patch: Partial<UserProfile> = {
      ...parsed.profilePatch,
      notes: parsed.notes,
    };
    if (!current) {
      this.upsertIdentity({
        userId,
        channelType: "manual",
        openId: null,
        source: "markdown-profile",
        profilePatch: {
          ...patch,
          visibility: patch.visibility ?? "private",
        },
      });
      return;
    }
    if (markdownMtime <= dbTime && !hasProfileDifference(current, patch)) {
      return;
    }
    this.upsertIdentity({
      userId,
      channelType: "manual",
      openId: null,
      source: "markdown-profile",
      profilePatch: {
        ...current,
        ...patch,
      },
    });
    } finally {
      this.markdownSyncing.delete(userId);
    }
  }

  private writeProfileMarkdown(profile: UserProfile): void {
    const markdownPath = this.profileMarkdownPath(profile.userId);
    mkdirSync(dirname(markdownPath), { recursive: true });
    writeFileSync(markdownPath, renderProfileMarkdown(profile), "utf8");
  }

  private writeExports(): void {
    const profiles = this.db.prepare(`
      SELECT user_id, name, gender, email, avatar, nickname, preferences, personality, role, timezone, notes, visibility, source, updated_at
      FROM ${TABLES.profiles}
      ORDER BY updated_at DESC
    `).all() as Array<Record<string, unknown>>;
    const bindings = this.db.prepare(`
      SELECT user_id, channel_type, open_id, external_user_id, union_id, source, updated_at
      FROM ${TABLES.bindings}
      ORDER BY updated_at DESC
    `).all() as Array<Record<string, unknown>>;
    writeFileSync(join(this.exportPath, "users.yaml"), renderYamlList(profiles), "utf8");
    writeFileSync(join(this.exportPath, "bindings.yaml"), renderYamlList(bindings), "utf8");
  }

  private ensureColumn(tableName: string, columnName: string, definition: string): void {
    const rows = this.db.prepare(`PRAGMA table_info(${tableName})`).all() as Array<Record<string, unknown>>;
    if (rows.some((row) => String(row.name) === columnName)) {
      return;
    }
    this.db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}`);
  }
}

class UserBindRuntime {
  private readonly store: UserBindStore;
  private readonly config: UserBindConfig;
  private readonly sessionCache = new Map<string, ResolvedIdentity>();
  private readonly bindingCache = new Map<string, { expiresAt: number; identity: ResolvedIdentity }>();
  private feishuScopeStatus: FeishuScopeStatus | null = null;
  private bitableMirror: { appToken: string | null; tableId: string | null } | null = null;
  private readonly feishuTokenCache = new Map<string, { token: string; expiresAt: number }>();

  constructor(private readonly host: HookApi, inputConfig: Partial<UserBindConfig> | undefined) {
    this.config = normalizeConfig(inputConfig);
    this.store = new UserBindStore(
      join(this.config.localStorePath, "profiles.sqlite"),
      this.config.exportPath,
      this.config.profileMarkdownRoot,
    );
  }

  close(): void {
    this.store.close();
  }

  register(): void {
    queueMicrotask(() => {
      try {
        bootstrapOpenClawHost(this.config);
      } catch {
        // Swallow host bootstrap errors so the runtime can still resolve identities.
      }
    });
    this.registerHooks();
    this.registerTools();
    exposeGlobalApi(this);
  }

  getIdentityForSession(sessionId: string): ResolvedIdentity | null {
    return this.sessionCache.get(sessionId) ?? null;
  }

  async resolveFromContext(context: unknown): Promise<ResolvedIdentity | null> {
    const parsed = parseIdentityContext(context);
    if (parsed.sessionId && !parsed.channelType) {
      return this.sessionCache.get(parsed.sessionId) ?? null;
    }
    if (!parsed.sessionId || !parsed.channelType) {
      return null;
    }

    const cacheKey = `${parsed.channelType}:${parsed.openId ?? parsed.sessionId}`;
    const cached = this.bindingCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) {
      this.sessionCache.set(parsed.sessionId, cached.identity);
      return cached.identity;
    }

    const binding = this.store.findBinding(parsed.channelType, parsed.openId);
    let userId = binding?.userId ?? null;
    let source = binding?.source ?? "local";

    let remoteProfilePatch: Partial<UserProfile> = {};
    if (parsed.channelType === "feishu" && parsed.openId) {
      const scopeStatus = await this.ensureFeishuScopeStatus();
      if (scopeStatus.missingIdentityScopes.length > 0) {
        const details = `Missing Feishu scopes: ${scopeStatus.missingIdentityScopes.join(", ")}`;
        logUserBindEvent("feishu-scope-missing", {
          openId: parsed.openId,
          missingScopes: scopeStatus.missingIdentityScopes,
        });
        this.store.recordIssue("feishu-scope-missing", details);
      }
      if (!userId) {
        const remote = await this.tryResolveFeishuUser(parsed.openId);
        if (remote?.userId) {
          userId = remote.userId;
          source = remote.source;
          remoteProfilePatch = remote.profilePatch;
        } else {
          this.store.recordIssue("feishu-resolution", `Failed to resolve real user id for ${parsed.openId}`);
        }
      }
    }

    if (!userId) {
      userId = `${parsed.channelType}:${parsed.openId ?? parsed.sessionId}`;
      source = "synthetic-fallback";
    }

    const profile = this.store.upsertIdentity({
      userId,
      channelType: parsed.channelType,
      openId: parsed.openId,
      source,
      profilePatch: {
        name: parsed.senderName,
        ...remoteProfilePatch,
      },
    });

    const identity: ResolvedIdentity = {
      sessionId: parsed.sessionId,
      userId,
      channelType: parsed.channelType,
      senderOpenId: parsed.openId,
      senderName: parsed.senderName,
      source,
      resolvedAt: new Date().toISOString(),
      profile,
    };

    this.sessionCache.set(parsed.sessionId, identity);
    this.bindingCache.set(cacheKey, {
      expiresAt: Date.now() + this.config.cacheTtlMs,
      identity,
    });
    if (parsed.channelType === "feishu") {
      await this.syncFeishuMirror(identity);
    }
    return identity;
  }

  async getMyProfile(context: unknown): Promise<UserProfile> {
    const identity = await this.requireCurrentIdentity(context);
    return identity.profile;
  }

  async updateMyProfile(context: unknown, patch: Partial<UserProfile>): Promise<UserProfile> {
    const identity = await this.requireCurrentIdentity(context);
    const updated = this.store.updateProfile(identity.userId, {
      ...patch,
      source: "self-update",
    });
    const nextIdentity = {
      ...identity,
      profile: updated,
    };
    this.sessionCache.set(identity.sessionId, nextIdentity);
    return updated;
  }

  async refreshMyBinding(context: unknown): Promise<ResolvedIdentity> {
    const identity = await this.requireCurrentIdentity(context);
    this.bindingCache.delete(`${identity.channelType}:${identity.senderOpenId ?? identity.sessionId}`);
    const refreshed = await this.resolveFromContext({
      sessionId: identity.sessionId,
      sender: { id: identity.senderOpenId, name: identity.senderName },
      channel: { type: identity.channelType },
    });
    if (!refreshed) {
      throw new Error("Unable to refresh binding for current session");
    }
    return refreshed;
  }

  async adminInstruction(
    toolName: string,
    instruction: string,
    context: unknown,
  ): Promise<unknown> {
    const requester = await this.resolveFromContext(context);
    const agentId = getAgentIdFromContext(context);
    if (!agentId || !this.config.adminAgents.includes(agentId)) {
      this.store.writeAudit({
        id: hashId(`${toolName}:${instruction}:${Date.now()}`),
        ts: new Date().toISOString(),
        agentId,
        requesterUserId: requester?.userId ?? null,
        toolName,
        instruction,
        resolvedAction: "deny",
        targetUserIds: [],
        decision: "denied",
        changedFields: [],
      });
      throw new Error("access denied: cross-user profile access is not allowed");
    }

    const parsed = parseAdminInstruction(instruction);
    const targetUserIds = parsed.userIds;
    let result: unknown;
    const changedFields: string[] = [];

    if (parsed.action === "query") {
      result = targetUserIds.map((userId) => this.store.getProfile(userId)).filter(Boolean);
    } else if (parsed.action === "edit") {
      if (targetUserIds.length !== 1) {
        throw new Error("admin edit requires exactly one target user");
      }
      result = this.store.updateProfile(targetUserIds[0], parsed.patch);
      changedFields.push(...Object.keys(parsed.patch));
    } else if (parsed.action === "merge") {
      if (targetUserIds.length !== 2) {
        throw new Error("admin merge requires source and target user ids");
      }
      result = this.store.mergeUsers(targetUserIds[0], targetUserIds[1]);
      changedFields.push("user_merge");
    } else if (parsed.action === "list_issues") {
      result = this.store.listIssues();
    } else if (parsed.action === "sync") {
      result = {
        synced: targetUserIds.map((userId) => ({
          userId,
          ok: true,
        })),
      };
    } else {
      throw new Error("unsupported admin instruction");
    }

    this.store.writeAudit({
      id: hashId(`${toolName}:${instruction}:${Date.now()}`),
      ts: new Date().toISOString(),
      agentId,
      requesterUserId: requester?.userId ?? null,
      toolName,
      instruction,
      resolvedAction: parsed.action,
      targetUserIds,
      decision: "allowed",
      changedFields,
    });
    return result;
  }

  private registerHooks(): void {
    this.host.registerHook?.(
      ["message:received", "message:preprocessed"],
      async (event) => {
        await this.resolveFromContext(event);
      },
      {
        name: "bamdra-user-bind-resolve",
        description: "Resolve runtime identity from channel sender metadata",
      },
    );
    this.host.on?.("before_prompt_build", async (_event, context) => {
      const identity = await this.resolveFromContext(context);
      if (!identity) {
        return;
      }
      return {
        context: [
          {
            type: "text",
            text: renderIdentityContext(identity),
          },
        ],
      };
    });
  }

  private registerTools(): void {
    const registerTool = this.host.registerTool?.bind(this.host);
    if (!registerTool) {
      return;
    }

    registerTool({
      name: "bamdra_user_bind_get_my_profile",
      description: "Get the current user's bound profile",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          sessionId: { type: "string" },
        },
      },
      execute: async (_id, params: unknown) => asTextResult(await this.getMyProfile(params)),
    });

    registerTool({
      name: "bamdra_user_bind_update_my_profile",
      description: "Update the current user's own profile fields",
      parameters: {
        type: "object",
        additionalProperties: false,
        required: ["sessionId"],
        properties: {
          sessionId: { type: "string" },
          nickname: { type: "string" },
          preferences: { type: "string" },
          personality: { type: "string" },
          role: { type: "string" },
          timezone: { type: "string" },
          notes: { type: "string" },
        },
      },
      execute: async (_id, params: Record<string, unknown>) =>
        asTextResult(await this.updateMyProfile(params, sanitizeProfilePatch(params))),
    });

    registerTool({
      name: "bamdra_user_bind_refresh_my_binding",
      description: "Refresh the current user's identity binding",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          sessionId: { type: "string" },
        },
      },
      execute: async (_id, params: unknown) => asTextResult(await this.refreshMyBinding(params)),
    });

    for (const toolName of ADMIN_TOOL_NAMES) {
      registerTool({
        name: toolName,
        description: `Administrative natural-language tool for ${toolName.replace("bamdra_user_bind_admin_", "")}`,
        parameters: {
          type: "object",
          additionalProperties: false,
          required: ["instruction"],
          properties: {
            instruction: { type: "string" },
            sessionId: { type: "string" },
            agentId: { type: "string" },
          },
        },
        execute: async (_id, params: Record<string, unknown>) =>
          asTextResult(await this.adminInstruction(toolName, String(params.instruction ?? ""), params)),
      });
    }
  }

  private async requireCurrentIdentity(context: unknown): Promise<ResolvedIdentity> {
    const identity = await this.resolveFromContext(context);
    if (!identity) {
      throw new Error("Unable to resolve current user identity");
    }
    return identity;
  }

  private async tryResolveFeishuUser(openId: string): Promise<FeishuUserResolution | null> {
    logUserBindEvent("feishu-resolution-start", { openId });
    const accounts = readFeishuAccountsFromOpenClawConfig();
    if (accounts.length === 0) {
      logUserBindEvent("feishu-resolution-skipped", { reason: "no-feishu-accounts-configured" });
      return null;
    }

    for (const account of accounts) {
      try {
        const token = await this.getFeishuAppAccessToken(account);
        const result = await feishuJsonRequest(
          account,
          `/open-apis/contact/v3/users/${encodeURIComponent(openId)}?user_id_type=open_id`,
          token,
        );
        const candidate = extractDeepString(result, [
          ["data", "user", "user_id"],
          ["user", "user_id"],
          ["data", "user_id"],
        ]);
        if (!candidate) {
          continue;
        }
        logUserBindEvent("feishu-resolution-success", {
          accountId: account.accountId,
          openId,
          userId: candidate,
        });
        return {
          userId: candidate,
          source: `feishu-api:${account.accountId}`,
          profilePatch: {
            name: extractDeepString(result, [["data", "user", "name"], ["user", "name"]]),
            email: extractDeepString(result, [["data", "user", "email"], ["user", "email"]]),
            avatar: extractDeepString(result, [["data", "user", "avatar", "avatar_origin"], ["user", "avatar", "avatar_origin"]]),
          },
        };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        logUserBindEvent("feishu-resolution-attempt-failed", {
          accountId: account.accountId,
          openId,
          message,
        });
      }
    }

    const executor = this.host.callTool ?? this.host.invokeTool;
    if (typeof executor === "function") {
      try {
        const result = await executor.call(this.host, "feishu_user_get", {
          user_id_type: "open_id",
          user_id: openId,
        }) as Record<string, unknown>;
        const candidate = extractDeepString(result, [
          ["data", "user", "user_id"],
          ["user", "user_id"],
          ["data", "user_id"],
        ]);
        if (candidate) {
          return {
            userId: candidate,
            source: "feishu-tool-fallback",
            profilePatch: {
              name: extractDeepString(result, [["data", "user", "name"], ["user", "name"]]),
            },
          };
        }
      } catch {
        // ignore host fallback failures
      }
    }

    logUserBindEvent("feishu-resolution-empty", { openId });
    return null;
  }

  private async ensureFeishuScopeStatus(): Promise<FeishuScopeStatus> {
    if (this.feishuScopeStatus) {
      return this.feishuScopeStatus;
    }
    const accounts = readFeishuAccountsFromOpenClawConfig();
    for (const account of accounts) {
      try {
        const token = await this.getFeishuAppAccessToken(account);
        const result = await feishuJsonRequest(
          account,
          "/open-apis/application/v6/scopes",
          token,
        );
        const scopes = extractScopes(result);
        this.feishuScopeStatus = {
          scopes,
          missingIdentityScopes: REQUIRED_FEISHU_IDENTITY_SCOPES.filter((scope) => !scopes.includes(scope)),
          hasDocumentAccess: scopes.some((scope) => scope.startsWith("bitable:") || scope.startsWith("drive:") || scope.startsWith("docx:") || scope.startsWith("docs:")),
        };
        logUserBindEvent("feishu-scopes-read", {
          accountId: account.accountId,
          ...this.feishuScopeStatus,
        });
        return this.feishuScopeStatus;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        logUserBindEvent("feishu-scopes-attempt-failed", { accountId: account.accountId, message });
      }
    }

    const executor = this.host.callTool ?? this.host.invokeTool;
    if (typeof executor === "function") {
      try {
        const result = await executor.call(this.host, "feishu_app_scopes", {}) as Record<string, unknown>;
        const scopes = extractScopes(result);
        this.feishuScopeStatus = {
          scopes,
          missingIdentityScopes: REQUIRED_FEISHU_IDENTITY_SCOPES.filter((scope) => !scopes.includes(scope)),
          hasDocumentAccess: scopes.some((scope) => scope.startsWith("bitable:") || scope.startsWith("drive:") || scope.startsWith("docx:") || scope.startsWith("docs:")),
        };
        logUserBindEvent("feishu-scopes-read", this.feishuScopeStatus);
        return this.feishuScopeStatus;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        logUserBindEvent("feishu-scopes-failed", { message });
        this.store.recordIssue("feishu-scope-read", message);
      }
    }

    this.feishuScopeStatus = {
      scopes: [],
      missingIdentityScopes: [...REQUIRED_FEISHU_IDENTITY_SCOPES],
      hasDocumentAccess: false,
    };
    return this.feishuScopeStatus;
  }

  private async getFeishuAppAccessToken(account: FeishuAccountCredentials): Promise<string> {
    const cached = this.feishuTokenCache.get(account.accountId);
    if (cached && cached.expiresAt > Date.now()) {
      return cached.token;
    }
    const base = resolveFeishuOpenApiBase(account.domain);
    const response = await fetch(`${base}/open-apis/auth/v3/app_access_token/internal`, {
      method: "POST",
      headers: {
        "content-type": "application/json; charset=utf-8",
      },
      body: JSON.stringify({
        app_id: account.appId,
        app_secret: account.appSecret,
      }),
    });
    const payload = await response.json() as Record<string, unknown>;
    if (!response.ok || Number(payload.code ?? 0) !== 0) {
      throw new Error(`Failed to get Feishu app access token for ${account.accountId}: ${JSON.stringify(payload)}`);
    }
    const token = asNullableString(payload.app_access_token);
    if (!token) {
      throw new Error(`Feishu app access token missing for ${account.accountId}`);
    }
    const expire = Number(payload.expire ?? 7200);
    this.feishuTokenCache.set(account.accountId, {
      token,
      expiresAt: Date.now() + Math.max(60, expire - 120) * 1000,
    });
    return token;
  }

  private async syncFeishuMirror(identity: ResolvedIdentity): Promise<void> {
    const scopeStatus = await this.ensureFeishuScopeStatus();
    if (!scopeStatus.hasDocumentAccess) {
      return;
    }
    const executor = this.host.callTool ?? this.host.invokeTool;
    if (typeof executor !== "function") {
      return;
    }
    try {
      const mirror = await this.ensureFeishuBitableMirror(executor.bind(this.host));
      if (!mirror.appToken || !mirror.tableId) {
        return;
      }
      const existing = await executor.call(this.host, "feishu_bitable_list_records", {
        app_token: mirror.appToken,
        table_id: mirror.tableId,
      }) as Record<string, unknown>;
      const recordId = findBitableRecordId(existing, identity.userId);
      const fields = {
        user_id: identity.userId,
        channel_type: identity.channelType,
        open_id: identity.senderOpenId,
        name: identity.profile.name,
        nickname: identity.profile.nickname,
        preferences: identity.profile.preferences,
        personality: identity.profile.personality,
        role: identity.profile.role,
        timezone: identity.profile.timezone,
        email: identity.profile.email,
        avatar: identity.profile.avatar,
      };
      if (recordId) {
        await executor.call(this.host, "feishu_bitable_update_record", {
          app_token: mirror.appToken,
          table_id: mirror.tableId,
          record_id: recordId,
          fields,
        });
      } else {
        await executor.call(this.host, "feishu_bitable_create_record", {
          app_token: mirror.appToken,
          table_id: mirror.tableId,
          fields,
        });
      }
      logUserBindEvent("feishu-bitable-sync-success", { userId: identity.userId });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logUserBindEvent("feishu-bitable-sync-failed", { userId: identity.userId, message });
      this.store.recordIssue("feishu-bitable-sync", message, identity.userId);
    }
  }

  private async ensureFeishuBitableMirror(
    executor: <TParams>(name: string, params: TParams) => Promise<unknown>,
  ): Promise<{ appToken: string | null; tableId: string | null }> {
    if (this.bitableMirror?.appToken && this.bitableMirror?.tableId) {
      return this.bitableMirror;
    }
    try {
      const app = await executor("feishu_bitable_create_app", { name: "Bamdra User Bind" }) as Record<string, unknown>;
      const appToken = extractDeepString(app, [
        ["data", "app", "app_token"],
        ["data", "app_token"],
        ["app", "app_token"],
        ["app_token"],
      ]);
      if (!appToken) {
        return { appToken: null, tableId: null };
      }
      const meta = await executor("feishu_bitable_get_meta", { app_token: appToken }) as Record<string, unknown>;
      const tableId = extractDeepString(meta, [
        ["data", "tables", "0", "table_id"],
        ["data", "items", "0", "table_id"],
        ["tables", "0", "table_id"],
      ]);
      if (!tableId) {
        this.store.recordIssue("feishu-bitable-init", "Unable to determine users table id from Feishu bitable metadata");
        return { appToken, tableId: null };
      }
      for (const fieldName of ["user_id", "channel_type", "open_id", "name", "nickname", "preferences", "personality", "role", "timezone", "email", "avatar"]) {
        try {
          await executor("feishu_bitable_create_field", {
            app_token: appToken,
            table_id: tableId,
            field_name: fieldName,
            type: 1,
          });
        } catch {
          // Field likely exists already.
        }
      }
      this.bitableMirror = { appToken, tableId };
      logUserBindEvent("feishu-bitable-ready", this.bitableMirror);
      return this.bitableMirror;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logUserBindEvent("feishu-bitable-init-failed", { message });
      this.store.recordIssue("feishu-bitable-init", message);
      return { appToken: null, tableId: null };
    }
  }
}

export function createUserBindPlugin(api: HookApi): UserBindRuntime {
  return new UserBindRuntime(api, api.pluginConfig ?? api.config ?? api.plugin?.config);
}

export function register(api: HookApi): void {
  createUserBindPlugin(api).register();
}

export async function activate(api: HookApi): Promise<void> {
  createUserBindPlugin(api).register();
}

function normalizeConfig(input: Partial<UserBindConfig> | undefined): UserBindConfig {
  const root = join(homedir(), ".openclaw", "data", "bamdra-user-bind");
  const storeRoot = input?.localStorePath ?? root;
  return {
    enabled: input?.enabled ?? true,
    localStorePath: storeRoot,
    exportPath: input?.exportPath ?? join(storeRoot, "exports"),
    profileMarkdownRoot: input?.profileMarkdownRoot ?? join(storeRoot, "profiles", "private"),
    cacheTtlMs: input?.cacheTtlMs ?? 30 * 60 * 1000,
    adminAgents: input?.adminAgents?.length ? input.adminAgents : ["main"],
  };
}

function exposeGlobalApi(runtime: UserBindRuntime): void {
  (globalThis as Record<string, unknown>)[GLOBAL_API_KEY] = {
    getIdentityForSession(sessionId: string) {
      return runtime.getIdentityForSession(sessionId);
    },
    async resolveIdentity(context: unknown) {
      return runtime.resolveFromContext(context);
    },
  };
}

function bootstrapOpenClawHost(config: UserBindConfig): void {
  const currentFile = fileURLToPath(import.meta.url);
  const runtimeDir = dirname(currentFile);
  const packageRoot = resolve(runtimeDir, "..");
  const openclawHome = resolve(homedir(), ".openclaw");
  const configPath = join(openclawHome, "openclaw.json");
  const extensionRoot = join(openclawHome, "extensions");
  const globalSkillsDir = join(openclawHome, "skills");
  const profileSkillSource = join(packageRoot, "skills", PROFILE_SKILL_ID);
  const adminSkillSource = join(packageRoot, "skills", ADMIN_SKILL_ID);
  const profileSkillTarget = join(globalSkillsDir, PROFILE_SKILL_ID);
  const adminSkillTarget = join(globalSkillsDir, ADMIN_SKILL_ID);

  if (!runtimeDir.startsWith(extensionRoot) || !existsSync(configPath)) {
    return;
  }

  mkdirSync(globalSkillsDir, { recursive: true });
  materializeBundledSkill(profileSkillSource, profileSkillTarget);
  materializeBundledSkill(adminSkillSource, adminSkillTarget);

  const original = readFileSync(configPath, "utf8");
  const parsed = JSON.parse(original) as Record<string, unknown>;
  const changed = ensureHostConfig(parsed, config, profileSkillTarget, adminSkillTarget);
  if (!changed) {
    return;
  }
  writeFileSync(configPath, `${JSON.stringify(parsed, null, 2)}\n`, "utf8");
}

function ensureHostConfig(
  config: Record<string, unknown>,
  pluginConfig: UserBindConfig,
  profileSkillTarget: string,
  adminSkillTarget: string,
): boolean {
  let changed = false;
  const plugins = ensureObject(config, "plugins");
  const entries = ensureObject(plugins, "entries");
  const load = ensureObject(plugins, "load");
  const tools = ensureObject(config, "tools");
  const skills = ensureObject(config, "skills");
  const skillsLoad = ensureObject(skills, "load");
  const agents = ensureObject(config, "agents");
  const entry = ensureObject(entries, PLUGIN_ID);
  const entryConfig = ensureObject(entry, "config");

  changed = ensureArrayIncludes(plugins, "allow", PLUGIN_ID) || changed;
  changed = ensureArrayIncludes(load, "paths", join(homedir(), ".openclaw", "extensions")) || changed;
  changed = ensureArrayIncludes(skillsLoad, "extraDirs", join(homedir(), ".openclaw", "skills")) || changed;

  if (entry.enabled !== true) {
    entry.enabled = true;
    changed = true;
  }
  changed = ensureToolNames(tools, [...SELF_TOOL_NAMES, ...ADMIN_TOOL_NAMES]) || changed;

  if (entryConfig.enabled !== true) {
    entryConfig.enabled = true;
    changed = true;
  }
  if (typeof entryConfig.localStorePath !== "string" || entryConfig.localStorePath.length === 0) {
    entryConfig.localStorePath = pluginConfig.localStorePath;
    changed = true;
  }
  if (typeof entryConfig.exportPath !== "string" || entryConfig.exportPath.length === 0) {
    entryConfig.exportPath = pluginConfig.exportPath;
    changed = true;
  }
  if (typeof entryConfig.profileMarkdownRoot !== "string" || entryConfig.profileMarkdownRoot.length === 0) {
    entryConfig.profileMarkdownRoot = pluginConfig.profileMarkdownRoot;
    changed = true;
  }
  if (!Array.isArray(entryConfig.adminAgents) || entryConfig.adminAgents.length === 0) {
    entryConfig.adminAgents = [...pluginConfig.adminAgents];
    changed = true;
  }

  changed = ensureAgentSkills(agents, PROFILE_SKILL_ID) || changed;
  changed = ensureAdminSkill(agents, ADMIN_SKILL_ID, pluginConfig.adminAgents) || changed;

  if (!existsSync(profileSkillTarget) || !existsSync(adminSkillTarget)) {
    changed = true;
  }

  return changed;
}

function materializeBundledSkill(sourceDir: string, targetDir: string): void {
  if (!existsSync(sourceDir) || existsSync(targetDir)) {
    return;
  }
  mkdirSync(dirname(targetDir), { recursive: true });
  cpSync(sourceDir, targetDir, { recursive: true });
}

function ensureToolNames(tools: Record<string, unknown>, values: string[]): boolean {
  let changed = false;
  for (const value of values) {
    changed = ensureArrayIncludes(tools, "allow", value) || changed;
  }
  return changed;
}

function ensureAgentSkills(agents: Record<string, unknown>, skillId: string): boolean {
  const list = Array.isArray(agents.list) ? agents.list : [];
  let changed = false;
  for (const item of list) {
    if (!item || typeof item !== "object") {
      continue;
    }
    const agent = item as Record<string, unknown>;
    const current = Array.isArray(agent.skills) ? [...(agent.skills as string[])] : [];
    if (!current.includes(skillId)) {
      current.push(skillId);
      agent.skills = current;
      changed = true;
    }
  }
  return changed;
}

function ensureAdminSkill(agents: Record<string, unknown>, skillId: string, adminAgents: string[]): boolean {
  const list = Array.isArray(agents.list) ? agents.list : [];
  let changed = false;
  let attached = false;
  for (const item of list) {
    if (!item || typeof item !== "object") {
      continue;
    }
    const agent = item as Record<string, unknown>;
    const agentId = getConfiguredAgentId(agent);
    if (!agentId || !adminAgents.includes(agentId)) {
      continue;
    }
    const current = Array.isArray(agent.skills) ? [...(agent.skills as string[])] : [];
    if (!current.includes(skillId)) {
      current.push(skillId);
      agent.skills = current;
      changed = true;
    }
    attached = true;
  }
  if (!attached && list.length > 0 && list[0] && typeof list[0] === "object") {
    const agent = list[0] as Record<string, unknown>;
    const current = Array.isArray(agent.skills) ? [...(agent.skills as string[])] : [];
    if (!current.includes(skillId)) {
      current.push(skillId);
      agent.skills = current;
      changed = true;
    }
  }
  return changed;
}

function mapProfileRow(row: Record<string, unknown>): UserProfile {
  return {
    userId: String(row.user_id),
    name: asNullableString(row.name),
    gender: asNullableString(row.gender),
    email: asNullableString(row.email),
    avatar: asNullableString(row.avatar),
    nickname: asNullableString(row.nickname),
    preferences: asNullableString(row.preferences),
    personality: asNullableString(row.personality),
    role: asNullableString(row.role),
    timezone: asNullableString(row.timezone),
    notes: asNullableString(row.notes),
    visibility: row.visibility === "shared" ? "shared" : "private",
    source: String(row.source ?? "local"),
    updatedAt: String(row.updated_at ?? new Date(0).toISOString()),
  };
}

function parseIdentityContext(context: unknown): {
  sessionId: string | null;
  channelType: string | null;
  openId: string | null;
  senderName: string | null;
} {
  const record = (context && typeof context === "object") ? context as Record<string, unknown> : {};
  const sender = findNestedRecord(record, ["sender"], ["message", "sender"], ["event", "sender"], ["payload", "sender"]);
  const message = findNestedRecord(record, ["message"], ["event", "message"], ["payload", "message"]);
  const session = findNestedRecord(record, ["session"], ["context", "session"]);
  const channel = findNestedRecord(record, ["channel"], ["message", "channel"], ["event", "channel"], ["payload", "channel"]);
  const metadata = findNestedRecord(record, ["metadata"]);
  const input = findNestedRecord(record, ["input"]);
  const conversation = findNestedRecord(record, ["conversation"]);
  const metadataText = asNullableString(record.text)
    ?? asNullableString(message.text)
    ?? asNullableString(record.content)
    ?? asNullableString(metadata.text)
    ?? asNullableString(input.text)
    ?? asNullableString(findNestedValue(record, ["message", "content", "text"]));
  const conversationInfo = metadataText ? extractTaggedJsonBlock(metadataText, "Conversation info (untrusted metadata)") : null;
  const senderInfo = metadataText ? extractTaggedJsonBlock(metadataText, "Sender (untrusted metadata)") : null;
  const senderIdFromText = metadataText ? extractRegexValue(metadataText, /"sender_id"\s*:\s*"([^"]+)"/) : null;
  const senderNameFromText = metadataText ? extractRegexValue(metadataText, /"sender"\s*:\s*"([^"]+)"/) : null;
  const senderNameFromMessageLine = metadataText ? extractRegexValue(metadataText, /\]\s*([^\n:：]{1,40})\s*[:：]/) : null;
  const sessionId = asNullableString(record.sessionKey)
    ?? asNullableString(record.sessionId)
    ?? asNullableString(session.id)
    ?? asNullableString(conversation.id)
    ?? asNullableString(metadata.sessionId)
    ?? asNullableString(input.sessionId)
    ?? asNullableString((input.session as Record<string, unknown> | undefined)?.id)
    ?? asNullableString((record.context as Record<string, unknown> | undefined)?.sessionId)
    ?? asNullableString(conversationInfo?.session_id)
    ?? asNullableString(conversationInfo?.message_id);
  const channelType = asNullableString(record.channelType)
    ?? asNullableString(channel.type)
    ?? asNullableString(metadata.channelType)
    ?? asNullableString((conversation as Record<string, unknown> | undefined)?.provider)
    ?? asNullableString(record.provider)
    ?? asNullableString(conversationInfo?.provider)
    ?? inferChannelTypeFromSessionId(sessionId);
  const openId = asNullableString(sender.id)
    ?? asNullableString(sender.open_id)
    ?? asNullableString(sender.openId)
    ?? asNullableString(sender.user_id)
    ?? asNullableString(senderInfo?.id)
    ?? asNullableString(conversationInfo?.sender_id)
    ?? senderIdFromText
    ?? extractOpenIdFromSessionId(sessionId);
  const senderName = asNullableString(sender.name)
    ?? asNullableString(sender.display_name)
    ?? asNullableString(senderInfo?.name)
    ?? asNullableString(conversationInfo?.sender)
    ?? senderNameFromText
    ?? senderNameFromMessageLine;

  return {
    sessionId,
    channelType,
    openId,
    senderName,
  };
}

function findNestedRecord(root: Record<string, unknown>, ...paths: string[][]): Record<string, unknown> {
  for (const path of paths) {
    const value = findNestedValue(root, path);
    if (value && typeof value === "object") {
      return value as Record<string, unknown>;
    }
  }
  return {};
}

function findNestedValue(root: Record<string, unknown>, path: string[]): unknown {
  let current: unknown = root;
  for (const part of path) {
    if (!current || typeof current !== "object") {
      return null;
    }
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}

function extractTaggedJsonBlock(text: string, label: string): Record<string, unknown> | null {
  const start = text.indexOf(label);
  if (start < 0) {
    return null;
  }
  const block = text.slice(start).match(/```json\s*([\s\S]*?)\s*```/i);
  if (!block) {
    return null;
  }
  try {
    const parsed = JSON.parse(block[1]);
    return parsed && typeof parsed === "object" ? parsed as Record<string, unknown> : null;
  } catch {
    return null;
  }
}

function inferChannelTypeFromSessionId(sessionId: string | null): string | null {
  if (!sessionId) {
    return null;
  }
  if (sessionId.includes(":feishu:")) {
    return "feishu";
  }
  if (sessionId.includes(":telegram:")) {
    return "telegram";
  }
  if (sessionId.includes(":whatsapp:")) {
    return "whatsapp";
  }
  return null;
}

function extractRegexValue(text: string, pattern: RegExp): string | null {
  const match = text.match(pattern);
  return match?.[1]?.trim() || null;
}

function extractOpenIdFromSessionId(sessionId: string | null): string | null {
  if (!sessionId) {
    return null;
  }
  const match = sessionId.match(/:([A-Za-z0-9_-]+)$/);
  return match?.[1] ?? null;
}

function getAgentIdFromContext(context: unknown): string | null {
  const record = (context && typeof context === "object") ? context as Record<string, unknown> : {};
  return asNullableString(record.agentId)
    ?? asNullableString((record.agent as Record<string, unknown> | undefined)?.id)
    ?? asNullableString((record.agent as Record<string, unknown> | undefined)?.name);
}

function sanitizeProfilePatch(params: Record<string, unknown>): Partial<UserProfile> {
  return {
    nickname: asNullableString(params.nickname),
    preferences: asNullableString(params.preferences),
    personality: asNullableString(params.personality),
    role: asNullableString(params.role),
    timezone: asNullableString(params.timezone),
    notes: asNullableString(params.notes),
  };
}

function parseAdminInstruction(instruction: string): {
  action: "query" | "edit" | "merge" | "list_issues" | "sync";
  userIds: string[];
  patch: Partial<UserProfile>;
} {
  const normalized = instruction.trim();
  if (/issue|问题|失败/i.test(normalized)) {
    return { action: "list_issues", userIds: [], patch: {} };
  }
  if (/merge|合并/i.test(normalized)) {
    const userIds = normalized.match(/user[:=]([A-Za-z0-9:_-]+)/g)?.map((item) => item.split(/[:=]/)[1]) ?? [];
    return { action: "merge", userIds, patch: {} };
  }
  if (/sync|重同步|同步/i.test(normalized)) {
    return { action: "sync", userIds: extractUserIds(normalized), patch: {} };
  }
  if (/edit|修改|改成|更新/i.test(normalized)) {
    return {
      action: "edit",
      userIds: extractUserIds(normalized),
      patch: extractProfilePatch(normalized),
    };
  }
  return {
    action: "query",
    userIds: extractUserIds(normalized),
    patch: {},
  };
}

function extractUserIds(input: string): string[] {
  return [...input.matchAll(/(?:user[:=]|用户[:：]?)([A-Za-z0-9:_-]+)/g)].map((match) => match[1]);
}

function extractProfilePatch(input: string): Partial<UserProfile> {
  const patch: Partial<UserProfile> = {};
  const nickname = input.match(/(?:nickname|称呼)[=:： ]([^,，]+)$/i) ?? input.match(/(?:nickname|称呼)[=:： ]([^,，]+)/i);
  const role = input.match(/(?:role|职责|角色)[=:： ]([^,，]+)/i);
  const preferences = input.match(/(?:preferences|偏好)[=:： ]([^,，]+)/i);
  const personality = input.match(/(?:personality|性格)[=:： ]([^,，]+)/i);
  const timezone = input.match(/(?:timezone|时区)[=:： ]([^,，]+)/i);
  if (nickname) {
    patch.nickname = nickname[1].trim();
  }
  if (role) {
    patch.role = role[1].trim();
  }
  if (preferences) {
    patch.preferences = preferences[1].trim();
  }
  if (personality) {
    patch.personality = personality[1].trim();
  }
  if (timezone) {
    patch.timezone = timezone[1].trim();
  }
  return patch;
}

function renderIdentityContext(identity: ResolvedIdentity): string {
  const lines = [
    `Resolved user id: ${identity.userId}`,
    `Channel: ${identity.channelType}`,
  ];
  if (identity.profile.name) {
    lines.push(`Name: ${identity.profile.name}`);
  }
  if (identity.profile.nickname) {
    lines.push(`Preferred address: ${identity.profile.nickname}`);
  }
  if (identity.profile.timezone) {
    lines.push(`Timezone: ${identity.profile.timezone}`);
  }
  if (identity.profile.preferences) {
    lines.push(`Preferences: ${identity.profile.preferences}`);
  }
  if (identity.profile.personality) {
    lines.push(`Personality: ${identity.profile.personality}`);
  }
  if (identity.profile.role) {
    lines.push(`Role: ${identity.profile.role}`);
  }
  if (identity.profile.notes) {
    lines.push(`Profile notes: ${identity.profile.notes}`);
  }
  return lines.join("\n");
}

function renderProfileMarkdown(profile: UserProfile): string {
  const frontmatter = [
    "---",
    `userId: ${escapeFrontmatter(profile.userId)}`,
    `name: ${escapeFrontmatter(profile.name)}`,
    `nickname: ${escapeFrontmatter(profile.nickname)}`,
    `timezone: ${escapeFrontmatter(profile.timezone ?? "Asia/Shanghai")}`,
    `preferences: ${escapeFrontmatter(profile.preferences ?? "幽默诙谐的对话风格，但是不过分")}`,
    `personality: ${escapeFrontmatter(profile.personality)}`,
    `role: ${escapeFrontmatter(profile.role)}`,
    `visibility: ${escapeFrontmatter(profile.visibility)}`,
    `source: ${escapeFrontmatter(profile.source)}`,
    `updatedAt: ${escapeFrontmatter(profile.updatedAt)}`,
    "---",
  ].join("\n");
  const notes = sanitizeProfileNotes(profile.notes) ?? defaultProfileNotes();
  return `${frontmatter}

# 用户画像

这个文件是当前用户的可编辑画像镜像。你可以直接修改上面的字段和下面的说明内容。

## 使用建议

- 常用称呼：例如“老板”
- 时区：例如 Asia/Shanghai
- 风格偏好：例如“幽默诙谐的对话风格，但是不过分”
- 角色信息：例如工作职责、协作边界、常见任务
- 其他备注：例如禁忌、习惯、输出偏好

## 备注

${notes}
`;
}

function parseProfileMarkdown(markdown: string): ParsedMarkdownProfile {
  const lines = markdown.split(/\r?\n/);
  const patch: Partial<UserProfile> = {};
  let notes: string | null = null;
  let index = 0;
  if (lines[index] === "---") {
    index += 1;
    while (index < lines.length && lines[index] !== "---") {
      const line = lines[index];
      const separatorIndex = line.indexOf(":");
      if (separatorIndex > 0) {
        const key = line.slice(0, separatorIndex).trim();
        const value = line.slice(separatorIndex + 1).trim();
        applyFrontmatterField(patch, key, value);
      }
      index += 1;
    }
    if (lines[index] === "---") {
      index += 1;
    }
  }
  const body = lines.slice(index).join("\n");
  const notesMatch = body.match(/##\s*备注\s*\n([\s\S]*)$/);
  if (notesMatch?.[1]) {
    notes = sanitizeProfileNotes(notesMatch[1]);
  }
  return {
    profilePatch: patch,
    notes,
  };
}

function applyFrontmatterField(patch: Partial<UserProfile>, key: string, value: string): void {
  const normalized = value === "null" ? null : value;
  if (key === "name") {
    patch.name = normalized;
  } else if (key === "nickname") {
    patch.nickname = normalized;
  } else if (key === "timezone") {
    patch.timezone = normalized;
  } else if (key === "preferences") {
    patch.preferences = normalized;
  } else if (key === "personality") {
    patch.personality = normalized;
  } else if (key === "role") {
    patch.role = normalized;
  } else if (key === "visibility") {
    patch.visibility = normalized === "shared" ? "shared" : "private";
  }
}

function hasProfileDifference(current: UserProfile, patch: Partial<UserProfile>): boolean {
  const entries = Object.entries(patch) as Array<[keyof UserProfile, UserProfile[keyof UserProfile]]>;
  return entries.some(([key, value]) => value != null && current[key] !== value);
}

function renderYamlList(rows: Array<Record<string, unknown>>): string {
  if (rows.length === 0) {
    return "[]\n";
  }
  return `${rows.map((row) => {
    const lines = ["-"];
    for (const [key, value] of Object.entries(row)) {
      const rendered = value == null ? "null" : JSON.stringify(value);
      lines.push(`  ${key}: ${rendered}`);
    }
    return lines.join("\n");
  }).join("\n")}\n`;
}

function extractDeepString(value: Record<string, unknown>, paths: string[][]): string | null {
  for (const path of paths) {
    let current: unknown = value;
    for (const key of path) {
      if (!current || typeof current !== "object") {
        current = null;
        break;
      }
      current = (current as Record<string, unknown>)[key];
    }
    const candidate = asNullableString(current);
    if (candidate) {
      return candidate;
    }
  }
  return null;
}

function asTextResult(value: unknown): UserBindToolResult {
  return {
    content: [
      {
        type: "text",
        text: typeof value === "string" ? value : JSON.stringify(value, null, 2),
      },
    ],
  };
}

function asNullableString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

function ensureObject(parent: Record<string, unknown>, key: string): Record<string, unknown> {
  const current = parent[key];
  if (current && typeof current === "object" && !Array.isArray(current)) {
    return current as Record<string, unknown>;
  }
  const next: Record<string, unknown> = {};
  parent[key] = next;
  return next;
}

function ensureArrayIncludes(parent: Record<string, unknown>, key: string, value: string): boolean {
  const current = Array.isArray(parent[key]) ? [...(parent[key] as string[])] : [];
  if (current.includes(value)) {
    if (!Array.isArray(parent[key])) {
      parent[key] = current;
    }
    return false;
  }
  current.push(value);
  parent[key] = current;
  return true;
}

function extractScopes(result: Record<string, unknown>): string[] {
  const candidates = [
    findNestedValue(result, ["data", "scopes"]),
    findNestedValue(result, ["scopes"]),
    findNestedValue(result, ["data", "items"]),
  ];
  for (const candidate of candidates) {
    if (!Array.isArray(candidate)) {
      continue;
    }
    const scopes = candidate.map((item) => {
      if (typeof item === "string") {
        return item;
      }
      if (item && typeof item === "object") {
        const record = item as Record<string, unknown>;
        const scope = record.scope ?? record.name;
        return typeof scope === "string" ? scope : "";
      }
      return "";
    }).filter(Boolean);
    if (scopes.length > 0) {
      return scopes;
    }
  }
  return [];
}

function findBitableRecordId(result: Record<string, unknown>, userId: string): string | null {
  const candidates = [
    findNestedValue(result, ["data", "items"]),
    findNestedValue(result, ["items"]),
    findNestedValue(result, ["data", "records"]),
  ];
  for (const candidate of candidates) {
    if (!Array.isArray(candidate)) {
      continue;
    }
    for (const item of candidate) {
      if (!item || typeof item !== "object") {
        continue;
      }
      const record = item as Record<string, unknown>;
      const fields = (record.fields && typeof record.fields === "object")
        ? record.fields as Record<string, unknown>
        : {};
      if (String(fields.user_id ?? "") !== userId) {
        continue;
      }
      const recordId = record.record_id ?? record.recordId ?? record.id;
      if (typeof recordId === "string" && recordId.trim()) {
        return recordId;
      }
    }
  }
  return null;
}

function readFeishuAccountsFromOpenClawConfig(): FeishuAccountCredentials[] {
  const openclawConfigPath = join(homedir(), ".openclaw", "openclaw.json");
  if (!existsSync(openclawConfigPath)) {
    return [];
  }
  try {
    const parsed = JSON.parse(readFileSync(openclawConfigPath, "utf8")) as Record<string, unknown>;
    const channels = (parsed.channels && typeof parsed.channels === "object")
      ? parsed.channels as Record<string, unknown>
      : {};
    const feishu = (channels.feishu && typeof channels.feishu === "object")
      ? channels.feishu as Record<string, unknown>
      : {};
    const accounts = (feishu.accounts && typeof feishu.accounts === "object")
      ? feishu.accounts as Record<string, unknown>
      : {};
    const topLevel = normalizeFeishuAccount("default", feishu, feishu);
    const values = Object.entries(accounts)
      .map(([accountId, value]) => normalizeFeishuAccount(accountId, value, feishu))
      .filter((item): item is FeishuAccountCredentials => item != null);
    if (topLevel && !values.some((item) => item.accountId === topLevel.accountId)) {
      values.unshift(topLevel);
    }
    return values;
  } catch (error) {
    logUserBindEvent("feishu-config-read-failed", {
      message: error instanceof Error ? error.message : String(error),
    });
    return [];
  }
}

function normalizeFeishuAccount(
  accountId: string,
  input: unknown,
  fallback: Record<string, unknown>,
): FeishuAccountCredentials | null {
  const record = (input && typeof input === "object") ? input as Record<string, unknown> : {};
  const enabled = record.enabled !== false && fallback.enabled !== false;
  const appId = asNullableString(record.appId) ?? asNullableString(fallback.appId);
  const appSecret = asNullableString(record.appSecret) ?? asNullableString(fallback.appSecret);
  const domain = asNullableString(record.domain) ?? asNullableString(fallback.domain) ?? "feishu";
  if (!enabled || !appId || !appSecret) {
    return null;
  }
  return { accountId, appId, appSecret, domain };
}

function resolveFeishuOpenApiBase(domain: string): string {
  if (domain === "lark") {
    return "https://open.larksuite.com";
  }
  if (domain === "feishu") {
    return "https://open.feishu.cn";
  }
  return domain.replace(/\/+$/, "");
}

async function feishuJsonRequest(
  account: FeishuAccountCredentials,
  path: string,
  appAccessToken: string,
  init?: RequestInit,
): Promise<Record<string, unknown>> {
  const base = resolveFeishuOpenApiBase(account.domain);
  const response = await fetch(`${base}${path}`, {
    ...init,
    headers: {
      authorization: `Bearer ${appAccessToken}`,
      "content-type": "application/json; charset=utf-8",
      ...(init?.headers ?? {}),
    },
  });
  const payload = await response.json() as Record<string, unknown>;
  if (!response.ok || Number(payload.code ?? 0) !== 0) {
    throw new Error(JSON.stringify(payload));
  }
  return payload;
}

function getConfiguredAgentId(agent: Record<string, unknown>): string | null {
  return asNullableString(agent.id) ?? asNullableString(agent.name) ?? asNullableString(agent.agentId);
}

function defaultProfileNotes(): string {
  return [
    "- 建议称呼：老板",
    "- 时区：Asia/Shanghai",
    "- 偏好：幽默诙谐的对话风格，但是不过分",
    "- 你可以在这里继续补充工作背景、表达习惯、禁忌和长期偏好。",
  ].join("\n");
}

function sanitizeProfileNotes(notes: string | null | undefined): string | null {
  const value = typeof notes === "string" ? notes.trim() : "";
  if (!value) {
    return null;
  }
  const normalized = value.replace(/\r/g, "");
  const marker = "## 备注";
  const lastMarkerIndex = normalized.lastIndexOf(marker);
  const sliced = lastMarkerIndex >= 0 ? normalized.slice(lastMarkerIndex + marker.length) : normalized;
  const cleaned = sliced
    .replace(/^[:：\s\n-]+/, "")
    .replace(/^#\s*用户画像[\s\S]*?##\s*备注\s*/m, "")
    .trim();
  return cleaned || null;
}

function escapeFrontmatter(value: string | null): string {
  if (!value) {
    return "null";
  }
  return value.replace(/\n/g, "\\n");
}

function joinNotes(primary: string | null, secondary: string | null): string | null {
  if (primary && secondary && primary !== secondary) {
    return `${primary}\n\n${secondary}`;
  }
  return primary ?? secondary ?? null;
}

function sanitizeFilename(value: string): string {
  return value.replace(/[^A-Za-z0-9._-]+/g, "_");
}

function hashId(value: string): string {
  return createHash("sha1").update(value).digest("hex").slice(0, 24);
}
