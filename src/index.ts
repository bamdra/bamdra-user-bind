import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { DatabaseSync } from "node:sqlite";

const PLUGIN_ID = "bamdra-user-bind";
const GLOBAL_API_KEY = "__OPENCLAW_BAMDRA_USER_BIND__";
const TABLES = {
  profiles: "bamdra_user_bind_profiles",
  bindings: "bamdra_user_bind_bindings",
  issues: "bamdra_user_bind_issues",
  audits: "bamdra_user_bind_audits",
} as const;

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

class UserBindStore {
  readonly db: DatabaseSync;

  constructor(
    private readonly dbPath: string,
    private readonly exportPath: string,
  ) {
    mkdirSync(dirname(dbPath), { recursive: true });
    mkdirSync(exportPath, { recursive: true });
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
  }

  close(): void {
    this.db.close();
  }

  getProfile(userId: string): UserProfile | null {
    const row = this.db.prepare(`
      SELECT user_id, name, gender, email, avatar, nickname, preferences, personality, role, visibility, source, updated_at
      FROM ${TABLES.profiles} WHERE user_id = ?
    `).get(userId) as Record<string, unknown> | undefined;
    return row ? mapProfileRow(row) : null;
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
    const now = new Date().toISOString();
    const current = this.getProfile(args.userId);
    const next: UserProfile = {
      userId: args.userId,
      name: args.profilePatch.name ?? current?.name ?? null,
      gender: args.profilePatch.gender ?? current?.gender ?? null,
      email: args.profilePatch.email ?? current?.email ?? null,
      avatar: args.profilePatch.avatar ?? current?.avatar ?? null,
      nickname: args.profilePatch.nickname ?? current?.nickname ?? null,
      preferences: args.profilePatch.preferences ?? current?.preferences ?? null,
      personality: args.profilePatch.personality ?? current?.personality ?? null,
      role: args.profilePatch.role ?? current?.role ?? null,
      visibility: args.profilePatch.visibility ?? current?.visibility ?? "private",
      source: args.source,
      updatedAt: now,
    };

    this.db.prepare(`
      INSERT INTO ${TABLES.profiles} (user_id, name, gender, email, avatar, nickname, preferences, personality, role, visibility, source, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(user_id) DO UPDATE SET
        name = excluded.name,
        gender = excluded.gender,
        email = excluded.email,
        avatar = excluded.avatar,
        nickname = excluded.nickname,
        preferences = excluded.preferences,
        personality = excluded.personality,
        role = excluded.role,
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
      next.visibility,
      next.source,
      next.updatedAt,
    );

    const bindingId = hashId(`${args.channelType}:${args.openId ?? args.userId}`);
    this.db.prepare(`
      INSERT INTO ${TABLES.bindings} (binding_id, user_id, channel_type, open_id, external_user_id, union_id, source, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(channel_type, open_id) DO UPDATE SET
        user_id = excluded.user_id,
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

  private writeExports(): void {
    const profiles = this.db.prepare(`
      SELECT user_id, name, gender, email, avatar, nickname, preferences, personality, role, visibility, source, updated_at
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
}

class UserBindRuntime {
  private readonly store: UserBindStore;
  private readonly config: UserBindConfig;
  private readonly sessionCache = new Map<string, ResolvedIdentity>();
  private readonly bindingCache = new Map<string, { expiresAt: number; identity: ResolvedIdentity }>();

  constructor(private readonly host: HookApi, inputConfig: Partial<UserBindConfig> | undefined) {
    this.config = normalizeConfig(inputConfig);
    this.store = new UserBindStore(
      join(this.config.localStorePath, "profiles.sqlite"),
      this.config.exportPath,
    );
  }

  close(): void {
    this.store.close();
  }

  register(): void {
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

    if (!userId && parsed.channelType === "feishu" && parsed.openId) {
      const remote = await this.tryResolveFeishuUser(parsed.openId);
      if (remote?.userId) {
        userId = remote.userId;
        source = remote.source;
      } else {
        this.store.recordIssue("feishu-resolution", `Failed to resolve real user id for ${parsed.openId}`);
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
      name: "user_bind_get_my_profile",
      description: "Get the current user's bound profile",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          sessionId: { type: "string" }
        }
      },
      execute: async (_id, params: unknown) => asTextResult(await this.getMyProfile(params)),
    });

    registerTool({
      name: "user_bind_update_my_profile",
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
          role: { type: "string" }
        }
      },
      execute: async (_id, params: Record<string, unknown>) =>
        asTextResult(await this.updateMyProfile(params, sanitizeProfilePatch(params))),
    });

    registerTool({
      name: "user_bind_refresh_my_binding",
      description: "Refresh the current user's identity binding",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          sessionId: { type: "string" }
        }
      },
      execute: async (_id, params: unknown) => asTextResult(await this.refreshMyBinding(params)),
    });

    for (const toolName of [
      "user_bind_admin_query",
      "user_bind_admin_edit",
      "user_bind_admin_merge",
      "user_bind_admin_list_issues",
      "user_bind_admin_sync",
    ]) {
      registerTool({
        name: toolName,
        description: `Administrative natural-language tool for ${toolName.replace("user_bind_admin_", "")}`,
        parameters: {
          type: "object",
          additionalProperties: false,
          required: ["instruction"],
          properties: {
            instruction: { type: "string" },
            sessionId: { type: "string" },
            agentId: { type: "string" }
          }
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

  private async tryResolveFeishuUser(openId: string): Promise<{ userId: string; source: string } | null> {
    const executor = this.host.callTool ?? this.host.invokeTool;
    if (typeof executor !== "function") {
      return null;
    }

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
      return candidate ? { userId: candidate, source: "feishu-api" } : null;
    } catch (error) {
      this.store.recordIssue("feishu-resolution", error instanceof Error ? error.message : String(error));
      return null;
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
  return {
    enabled: input?.enabled ?? true,
    localStorePath: input?.localStorePath ?? root,
    exportPath: input?.exportPath ?? join(root, "exports"),
    cacheTtlMs: input?.cacheTtlMs ?? 30 * 60 * 1000,
    adminAgents: input?.adminAgents ?? [],
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
  const sender = ((record.sender && typeof record.sender === "object") ? record.sender : {}) as Record<string, unknown>;
  const message = ((record.message && typeof record.message === "object") ? record.message : {}) as Record<string, unknown>;
  const sessionId = asNullableString(record.sessionId)
    ?? asNullableString(record.sessionKey)
    ?? asNullableString((record.session as Record<string, unknown> | undefined)?.id)
    ?? asNullableString((record.context as Record<string, unknown> | undefined)?.sessionId);
  const channelType = asNullableString(record.channelType)
    ?? asNullableString((record.channel as Record<string, unknown> | undefined)?.type)
    ?? asNullableString((message.channel as Record<string, unknown> | undefined)?.type)
    ?? asNullableString(record.provider);
  const openId = asNullableString(sender.id)
    ?? asNullableString(sender.open_id)
    ?? asNullableString(sender.openId)
    ?? asNullableString((message.sender as Record<string, unknown> | undefined)?.id);
  const senderName = asNullableString(sender.name)
    ?? asNullableString(sender.display_name)
    ?? asNullableString((message.sender as Record<string, unknown> | undefined)?.name);

  return {
    sessionId,
    channelType,
    openId,
    senderName,
  };
}

function getAgentIdFromContext(context: unknown): string | null {
  const record = (context && typeof context === "object") ? context as Record<string, unknown> : {};
  return asNullableString(record.agentId)
    ?? asNullableString((record.agent as Record<string, unknown> | undefined)?.id);
}

function sanitizeProfilePatch(params: Record<string, unknown>): Partial<UserProfile> {
  return {
    nickname: asNullableString(params.nickname),
    preferences: asNullableString(params.preferences),
    personality: asNullableString(params.personality),
    role: asNullableString(params.role),
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
  if (identity.profile.preferences) {
    lines.push(`Preferences: ${identity.profile.preferences}`);
  }
  if (identity.profile.personality) {
    lines.push(`Personality: ${identity.profile.personality}`);
  }
  if (identity.profile.role) {
    lines.push(`Role: ${identity.profile.role}`);
  }
  return lines.join("\n");
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

function hashId(value: string): string {
  return createHash("sha1").update(value).digest("hex").slice(0, 24);
}
