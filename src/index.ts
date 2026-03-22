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
const SELF_TOOL_ALIASES = [
  "user_bind_get_my_profile",
  "user_bind_update_my_profile",
  "user_bind_refresh_my_binding",
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
const CHANNELS_WITH_NATIVE_STABLE_IDS = new Set([
  "telegram",
  "whatsapp",
  "discord",
  "googlechat",
  "slack",
  "mattermost",
  "signal",
  "imessage",
  "msteams",
]);

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
  updatedAt: string | null;
  source: string | null;
  syncHash: string | null;
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

interface OpenAiCompatibleModelConfig {
  providerId: string;
  modelId: string;
  baseUrl: string;
  apiKey: string;
}

type ProfileFieldOperation = "replace" | "append" | "remove";

interface ProfilePatchOperations {
  nickname?: ProfileFieldOperation;
  preferences?: ProfileFieldOperation;
  personality?: ProfileFieldOperation;
  role?: ProfileFieldOperation;
  timezone?: ProfileFieldOperation;
  notes?: ProfileFieldOperation;
}

interface SemanticProfileExtraction {
  shouldUpdate: boolean;
  confidence: number;
  patch: Partial<UserProfile>;
  operations?: ProfilePatchOperations;
}

interface PendingBindingResolution {
  channelType: string;
  openId: string;
  reason: string;
  attempts: number;
  nextAttemptAt: number;
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
    this.migrateChannelScopedUserIds();
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

  reconcileProvisionalIdentity(channelType: string, openId: string | null, stableUserId: string): UserProfile | null {
    if (!openId) {
      return null;
    }
    const provisionalUserId = createProvisionalUserId(channelType, openId);
    const scopedStableUserId = scopeUserId(channelType, stableUserId);
    if (provisionalUserId === scopedStableUserId) {
      return this.getProfile(scopedStableUserId);
    }
    const provisional = this.getProfile(provisionalUserId);
    const stable = this.getProfile(scopedStableUserId);
    if (!provisional || !stable) {
      return stable;
    }
    return this.mergeUsers(provisionalUserId, scopedStableUserId);
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
    const scopedUserId = scopeUserId(args.channelType, args.userId);
    const now = new Date().toISOString();
    const current = this.getProfileFromDatabase(scopedUserId);
    const externalUserId = args.openId ?? getExternalUserId(args.channelType, args.userId);
    const next: UserProfile = {
      userId: scopedUserId,
      name: args.profilePatch.name ?? current?.name ?? null,
      gender: args.profilePatch.gender ?? current?.gender ?? null,
      email: args.profilePatch.email ?? current?.email ?? null,
      avatar: args.profilePatch.avatar ?? current?.avatar ?? null,
      nickname: args.profilePatch.nickname ?? current?.nickname ?? null,
      preferences: args.profilePatch.preferences ?? current?.preferences ?? null,
      personality: args.profilePatch.personality ?? current?.personality ?? null,
      role: args.profilePatch.role ?? current?.role ?? null,
      timezone: args.profilePatch.timezone ?? current?.timezone ?? getServerTimezone(),
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
        scopedUserId,
        args.channelType,
        args.openId,
        externalUserId,
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
      const patch: Partial<UserProfile> = {
        ...parsed.profilePatch,
        notes: parsed.notes,
      };
      if (!current) {
        this.upsertIdentity({
          userId,
          channelType: "manual",
          openId: null,
          source: parsed.source ?? "markdown-profile",
          profilePatch: {
            ...patch,
            visibility: patch.visibility ?? "private",
          },
        });
        return;
      }

      const markdownMtime = statSync(markdownPath).mtime.toISOString();
      const markdownHash = computeProfilePayloadHash({
        name: patch.name ?? null,
        nickname: patch.nickname ?? null,
        timezone: patch.timezone ?? null,
        preferences: patch.preferences ?? null,
        personality: patch.personality ?? null,
        role: patch.role ?? null,
        visibility: patch.visibility ?? current.visibility,
      }, patch.notes ?? null);
      const currentHash = computeProfilePayloadHash({
        name: current.name,
        nickname: current.nickname,
        timezone: current.timezone,
        preferences: current.preferences,
        personality: current.personality,
        role: current.role,
        visibility: current.visibility,
      }, current.notes);

      if (markdownHash === currentHash) {
        return;
      }
      if (parsed.syncHash && parsed.syncHash === markdownHash) {
        return;
      }
      const dbTime = current.updatedAt;
      if (parsed.updatedAt && parsed.updatedAt <= dbTime && markdownMtime <= dbTime) {
        return;
      }
      if (!parsed.syncHash && markdownMtime <= dbTime) {
        return;
      }
      this.upsertIdentity({
        userId,
        channelType: "manual",
        openId: null,
        source: parsed.source ?? "markdown-profile",
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

  private migrateChannelScopedUserIds(): void {
    const rows = this.db.prepare(`
      SELECT DISTINCT user_id, channel_type
      FROM ${TABLES.bindings}
      WHERE user_id IS NOT NULL AND channel_type IS NOT NULL
    `).all() as Array<Record<string, unknown>>;
    const renames = rows
      .map((row) => {
        const currentUserId = String(row.user_id ?? "");
        const channelType = String(row.channel_type ?? "");
        const scopedUserId = scopeUserId(channelType, currentUserId);
        if (!currentUserId || !channelType || scopedUserId === currentUserId) {
          return null;
        }
        return { currentUserId, scopedUserId };
      })
      .filter((item): item is { currentUserId: string; scopedUserId: string } => item != null);
    if (renames.length === 0) {
      return;
    }
    for (const { currentUserId, scopedUserId } of renames) {
      this.renameUserId(currentUserId, scopedUserId);
    }
    this.writeExports();
  }

  private renameUserId(fromUserId: string, toUserId: string): void {
    if (fromUserId === toUserId) {
      return;
    }
    const existingTarget = this.getProfileFromDatabase(toUserId);
    const sourceProfile = this.getProfileFromDatabase(fromUserId);
    if (sourceProfile && !existingTarget) {
      this.db.prepare(`UPDATE ${TABLES.profiles} SET user_id = ? WHERE user_id = ?`).run(toUserId, fromUserId);
    } else if (sourceProfile && existingTarget) {
      const merged = {
        ...sourceProfile,
        ...existingTarget,
        userId: toUserId,
        updatedAt: new Date().toISOString(),
        source: existingTarget.source || sourceProfile.source,
        notes: joinNotes(existingTarget.notes, sourceProfile.notes),
      };
      this.db.prepare(`DELETE FROM ${TABLES.profiles} WHERE user_id = ?`).run(fromUserId);
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
        merged.userId,
        merged.name,
        merged.gender,
        merged.email,
        merged.avatar,
        merged.nickname,
        merged.preferences,
        merged.personality,
        merged.role,
        merged.timezone,
        merged.notes,
        merged.visibility,
        merged.source,
        merged.updatedAt,
      );
    }
    this.db.prepare(`UPDATE ${TABLES.bindings} SET user_id = ?, external_user_id = ?, updated_at = ? WHERE user_id = ?`).run(
      toUserId,
      getExternalUserId(extractChannelFromScopedUserId(toUserId) ?? "manual", toUserId),
      new Date().toISOString(),
      fromUserId,
    );
    const oldMarkdownPath = this.profileMarkdownPath(fromUserId);
    const newMarkdownPath = this.profileMarkdownPath(toUserId);
    if (existsSync(oldMarkdownPath) && !existsSync(newMarkdownPath)) {
      mkdirSync(dirname(newMarkdownPath), { recursive: true });
      cpSync(oldMarkdownPath, newMarkdownPath);
    }
  }
}

class UserBindRuntime {
  private readonly store: UserBindStore;
  private readonly config: UserBindConfig;
  private readonly sessionCache = new Map<string, ResolvedIdentity>();
  private readonly bindingCache = new Map<string, { expiresAt: number; identity: ResolvedIdentity }>();
  private readonly semanticCaptureCache = new Map<string, number>();
  private readonly semanticCaptureInFlight = new Set<string>();
  private readonly pendingBindingResolutions = new Map<string, PendingBindingResolution>();
  private readonly feishuScopeStatusCache = new Map<string, FeishuScopeStatus>();
  private bitableMirror: { appToken: string | null; tableId: string | null } | null = null;
  private readonly feishuTokenCache = new Map<string, { token: string; expiresAt: number }>();
  private pendingBindingTimer: ReturnType<typeof setInterval> | null = null;
  private pendingBindingKickTimer: ReturnType<typeof setTimeout> | null = null;
  private pendingBindingSweepInFlight = false;

  constructor(private readonly host: HookApi, inputConfig: Partial<UserBindConfig> | undefined) {
    this.config = normalizeConfig(inputConfig);
    this.store = new UserBindStore(
      join(this.config.localStorePath, "profiles.sqlite"),
      this.config.exportPath,
      this.config.profileMarkdownRoot,
    );
  }

  close(): void {
    if (this.pendingBindingTimer) {
      clearInterval(this.pendingBindingTimer);
      this.pendingBindingTimer = null;
    }
    if (this.pendingBindingKickTimer) {
      clearTimeout(this.pendingBindingKickTimer);
      this.pendingBindingKickTimer = null;
    }
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
    this.startPendingBindingWorker();
    exposeGlobalApi(this);
  }

  getIdentityForSession(sessionId: string): ResolvedIdentity | null {
    return this.sessionCache.get(sessionId) ?? null;
  }

  async resolveFromContext(context: unknown): Promise<ResolvedIdentity | null> {
    const parsed = parseIdentityContext(enrichIdentityContext(context));
    if (parsed.sessionId && !parsed.channelType) {
      const cached = this.sessionCache.get(parsed.sessionId) ?? null;
      if (cached && isProvisionalScopedUserId(cached.userId) && cached.senderOpenId) {
        return this.resolveFromContext({
          sessionId: parsed.sessionId,
          channel: { type: cached.channelType },
          sender: { id: cached.senderOpenId, name: cached.senderName },
        });
      }
      return cached;
    }
    if (!parsed.sessionId || !parsed.channelType) {
      return null;
    }

    const cacheKey = `${parsed.channelType}:${parsed.openId ?? parsed.sessionId}`;
    const cached = this.bindingCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) {
      if (parsed.openId && (isProvisionalScopedUserId(cached.identity.userId) || cached.identity.source === "synthetic-fallback")) {
        const rebound = this.store.findBinding(parsed.channelType, parsed.openId);
        if (!rebound || rebound.userId === cached.identity.userId) {
          this.sessionCache.set(parsed.sessionId, cached.identity);
          return cached.identity;
        }
        this.bindingCache.delete(cacheKey);
      } else {
        this.sessionCache.set(parsed.sessionId, cached.identity);
        return cached.identity;
      }
    }

    const binding = this.store.findBinding(parsed.channelType, parsed.openId);
    let userId = binding?.userId ?? null;
    let source = binding?.source ?? "local";
    const provisionalUserId = parsed.openId
      ? createProvisionalUserId(parsed.channelType, parsed.openId)
      : null;

    let remoteProfilePatch: Partial<UserProfile> = {};
    if (parsed.channelType === "feishu" && parsed.openId) {
      if (!userId) {
        const remote = await this.tryResolveFeishuUser(parsed.openId);
        if (remote?.userId) {
          userId = remote.userId;
          source = remote.source;
          remoteProfilePatch = remote.profilePatch;
        } else if (remote?.source === "feishu-contact-scope-missing") {
          source = remote.source;
          remoteProfilePatch = remote.profilePatch;
          this.enqueuePendingBindingResolution(parsed.channelType, parsed.openId, remote.source);
        } else {
          this.store.recordIssue("feishu-resolution", `Failed to resolve real user id for ${parsed.openId}`);
          this.enqueuePendingBindingResolution(parsed.channelType, parsed.openId, "feishu-resolution-miss");
        }
      }
    }

    const profilePatch = {
      name: parsed.senderName,
      ...remoteProfilePatch,
    };
    if (!userId && parsed.openId && parsed.channelType && CHANNELS_WITH_NATIVE_STABLE_IDS.has(parsed.channelType)) {
      userId = `${parsed.channelType}:${parsed.openId}`;
      source = "channel-native";
    }
    if (!userId && provisionalUserId) {
      userId = provisionalUserId;
      source = "provisional-openid";
      this.enqueuePendingBindingResolution(parsed.channelType, parsed.openId, source);
    }
    const shouldPersistIdentity = Boolean(userId);
    if (!userId) {
      userId = createEphemeralUserId(parsed.channelType, parsed.openId, parsed.sessionId);
      source = "synthetic-fallback";
    }

    if (shouldPersistIdentity) {
      this.store.getProfile(userId);
    }

    const profile = shouldPersistIdentity
      ? (() => {
        let persisted = this.store.upsertIdentity({
          userId,
          channelType: parsed.channelType,
          openId: parsed.openId,
          source,
          profilePatch,
        });
        if (
          provisionalUserId
          && persisted.userId !== provisionalUserId
          && source !== "provisional-openid"
        ) {
          const reconciled = this.store.reconcileProvisionalIdentity(parsed.channelType, parsed.openId, persisted.userId);
          if (reconciled) {
            persisted = reconciled;
            logUserBindEvent("provisional-profile-merged", {
              provisionalUserId,
              stableUserId: persisted.userId,
              sessionId: parsed.sessionId,
            });
          }
        }
        return persisted;
      })()
      : buildLightweightProfile({
        userId,
        source,
        current: null,
        profilePatch,
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
    if (shouldPersistIdentity && parsed.channelType === "feishu") {
      await this.syncFeishuMirror(identity);
    }
    return identity;
  }

  private startPendingBindingWorker(): void {
    if (this.pendingBindingTimer) {
      return;
    }
    this.pendingBindingTimer = setInterval(() => {
      void this.runPendingBindingSweep();
    }, 60 * 1000);
    this.pendingBindingTimer.unref?.();
  }

  private schedulePendingBindingSweep(delayMs: number): void {
    if (this.pendingBindingKickTimer) {
      return;
    }
    this.pendingBindingKickTimer = setTimeout(() => {
      this.pendingBindingKickTimer = null;
      void this.runPendingBindingSweep();
    }, delayMs);
    this.pendingBindingKickTimer.unref?.();
  }

  private enqueuePendingBindingResolution(channelType: string, openId: string | null, reason: string): void {
    if (!openId) {
      return;
    }
    const key = `${channelType}:${openId}`;
    const current = this.pendingBindingResolutions.get(key);
    if (current) {
      current.reason = reason;
      current.nextAttemptAt = Math.min(current.nextAttemptAt, Date.now() + 5_000);
      this.pendingBindingResolutions.set(key, current);
    } else {
      this.pendingBindingResolutions.set(key, {
        channelType,
        openId,
        reason,
        attempts: 0,
        nextAttemptAt: Date.now() + 5_000,
      });
    }
    this.schedulePendingBindingSweep(5_000);
  }

  async runPendingBindingSweep(): Promise<void> {
    if (this.pendingBindingSweepInFlight || this.pendingBindingResolutions.size === 0) {
      return;
    }
    this.pendingBindingSweepInFlight = true;
    try {
      const now = Date.now();
      for (const [key, pending] of this.pendingBindingResolutions.entries()) {
        if (pending.nextAttemptAt > now) {
          continue;
        }
        const existing = this.store.findBinding(pending.channelType, pending.openId);
        if (existing && !isProvisionalScopedUserId(existing.userId)) {
          this.store.upsertIdentity({
            userId: existing.userId,
            channelType: pending.channelType,
            openId: pending.openId,
            source: existing.source,
            profilePatch: {},
          });
          this.store.reconcileProvisionalIdentity(pending.channelType, pending.openId, existing.userId);
          this.dropCachedOpenIdIdentity(pending.channelType, pending.openId);
          this.pendingBindingResolutions.delete(key);
          logUserBindEvent("pending-binding-reconciled", {
            channelType: pending.channelType,
            openId: pending.openId,
            userId: existing.userId,
          });
          continue;
        }
        if (pending.channelType !== "feishu") {
          this.pendingBindingResolutions.delete(key);
          continue;
        }
        const remote = await this.tryResolveFeishuUser(pending.openId);
        if (remote?.userId) {
          this.store.upsertIdentity({
            userId: remote.userId,
            channelType: pending.channelType,
            openId: pending.openId,
            source: remote.source,
            profilePatch: remote.profilePatch,
          });
          this.store.reconcileProvisionalIdentity(pending.channelType, pending.openId, remote.userId);
          this.dropCachedOpenIdIdentity(pending.channelType, pending.openId);
          this.pendingBindingResolutions.delete(key);
          logUserBindEvent("pending-binding-resolved", {
            channelType: pending.channelType,
            openId: pending.openId,
            userId: remote.userId,
            source: remote.source,
          });
          continue;
        }
        pending.attempts += 1;
        pending.nextAttemptAt = Date.now() + getPendingBindingRetryDelayMs(remote?.source ?? pending.reason, pending.attempts);
        this.pendingBindingResolutions.set(key, pending);
      }
    } finally {
      this.pendingBindingSweepInFlight = false;
    }
  }

  private dropCachedOpenIdIdentity(channelType: string, openId: string): void {
    const cacheKey = `${channelType}:${openId}`;
    this.bindingCache.delete(cacheKey);
    for (const [sessionId, identity] of this.sessionCache.entries()) {
      if (identity.channelType === channelType && identity.senderOpenId === openId) {
        this.sessionCache.delete(sessionId);
      }
    }
  }

  async getMyProfile(context: unknown): Promise<UserProfile> {
    const identity = await this.requireCurrentIdentity(context);
    return identity.profile;
  }

  async updateMyProfile(
    context: unknown,
    patch: Partial<UserProfile>,
    operations?: ProfilePatchOperations,
  ): Promise<UserProfile> {
    const identity = await this.requireCurrentIdentity(context);
    const fallbackOperations = extractProfilePatchOperations((context && typeof context === "object") ? context as Record<string, unknown> : {});
    const nextPatch = applyProfilePatchOperations(identity.profile, patch, operations ?? fallbackOperations);
    const updated = identity.source === "synthetic-fallback"
      ? this.store.upsertIdentity({
        userId: createStableLocalUserId(identity.channelType, identity.senderOpenId, identity.sessionId),
        channelType: identity.channelType,
        openId: identity.senderOpenId,
        source: "self-bootstrap",
        profilePatch: {
          ...identity.profile,
          ...nextPatch,
        },
      })
      : this.store.updateProfile(identity.userId, {
        ...nextPatch,
        source: "self-update",
      });
    const nextIdentity = {
      ...identity,
      userId: updated.userId,
      source: updated.source,
      profile: updated,
    };
    this.sessionCache.set(identity.sessionId, nextIdentity);
    this.bindingCache.set(`${identity.channelType}:${identity.senderOpenId ?? identity.sessionId}`, {
      expiresAt: Date.now() + this.config.cacheTtlMs,
      identity: nextIdentity,
    });
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

  private async captureProfileFromMessage(context: unknown, identity: ResolvedIdentity): Promise<void> {
    const messageText = extractUserUtterance(context);
    if (!messageText) {
      logUserBindEvent("semantic-profile-capture-skipped-no-utterance", {
        userId: identity.userId,
        sessionId: identity.sessionId,
      });
      return;
    }
    const fingerprint = hashId(`${identity.sessionId}:${messageText}`);
    this.pruneSemanticCaptureCache();
    if (this.semanticCaptureInFlight.has(fingerprint)) {
      return;
    }
    const processedAt = this.semanticCaptureCache.get(fingerprint);
    if (processedAt && processedAt > Date.now() - 12 * 60 * 60 * 1000) {
      return;
    }
    this.semanticCaptureInFlight.add(fingerprint);
    try {
      const extraction = await inferSemanticProfileExtraction(messageText, identity.profile);
      if (!extraction?.shouldUpdate) {
        logUserBindEvent("semantic-profile-capture-noop", {
          userId: identity.userId,
          sessionId: identity.sessionId,
          confidence: extraction?.confidence ?? 0,
          messagePreview: messageText.slice(0, 120),
        });
        this.semanticCaptureCache.set(fingerprint, Date.now());
        return;
      }
      const { patch, operations } = cleanupSemanticProfilePatch(
        extraction.patch,
        identity.profile,
        extraction.operations,
      );
      if (Object.keys(patch).length === 0) {
        this.semanticCaptureCache.set(fingerprint, Date.now());
        return;
      }
      await this.updateMyProfile(
        { sessionId: identity.sessionId },
        {
          ...patch,
          source: "semantic-self-update",
        },
        operations,
      );
      logUserBindEvent("semantic-profile-capture-success", {
        userId: identity.userId,
        sessionId: identity.sessionId,
        fields: Object.keys(patch),
        operations,
        confidence: extraction.confidence,
      });
      this.semanticCaptureCache.set(fingerprint, Date.now());
    } catch (error) {
      logUserBindEvent("semantic-profile-capture-failed", {
        userId: identity.userId,
        sessionId: identity.sessionId,
        message: error instanceof Error ? error.message : String(error),
      });
    } finally {
      this.semanticCaptureInFlight.delete(fingerprint);
    }
  }

  private pruneSemanticCaptureCache(): void {
    const cutoff = Date.now() - 12 * 60 * 60 * 1000;
    for (const [fingerprint, ts] of this.semanticCaptureCache.entries()) {
      if (ts < cutoff) {
        this.semanticCaptureCache.delete(fingerprint);
      }
    }
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
        const identity = await this.resolveFromContext(event);
        if (identity) {
          await this.captureProfileFromMessage(event, identity);
        }
      },
      {
        name: "bamdra-user-bind-resolve",
        description: "Resolve runtime identity from channel sender metadata",
      },
    );
    this.host.on?.("before_prompt_build", async (event, context) => {
      const identity = await this.resolveFromContext(context);
      if (!identity) {
        return;
      }
      const captureContext = mergeSemanticCaptureContext(event, context);
      await this.captureProfileFromMessage(captureContext, identity);
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

    const getMyProfileExecute = async (_id: string, params: unknown) =>
      asTextResult(await this.getMyProfile(params));
    const updateMyProfileExecute = async (_id: string, params: Record<string, unknown>) =>
      asTextResult(await this.updateMyProfile(
        params,
        sanitizeProfilePatch(params),
        extractProfilePatchOperations(params),
      ));
    const refreshMyBindingExecute = async (_id: string, params: unknown) =>
      asTextResult(await this.refreshMyBinding(params));

    registerTool({
      name: "bamdra_user_bind_get_my_profile",
      description:
        "Get the current user's bound profile before replying so nickname, timezone, and stable preferences can be used naturally in the response",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          sessionId: { type: "string" },
        },
      },
      execute: getMyProfileExecute,
    });

    registerTool({
      name: "bamdra_user_bind_update_my_profile",
      description:
        "Immediately write the current user's stable preferences into their profile when they clearly provide them, such as nickname, communication style, timezone, role, or durable notes",
      parameters: {
        type: "object",
        additionalProperties: false,
        required: ["sessionId"],
        properties: {
          sessionId: { type: "string" },
          nickname: { type: "string" },
          nicknameOperation: { type: "string", enum: ["replace", "append", "remove"] },
          preferences: { type: "string" },
          preferencesOperation: { type: "string", enum: ["replace", "append", "remove"] },
          personality: { type: "string" },
          personalityOperation: { type: "string", enum: ["replace", "append", "remove"] },
          role: { type: "string" },
          roleOperation: { type: "string", enum: ["replace", "append", "remove"] },
          timezone: { type: "string" },
          timezoneOperation: { type: "string", enum: ["replace", "append", "remove"] },
          notes: { type: "string" },
          notesOperation: { type: "string", enum: ["replace", "append", "remove"] },
        },
      },
      execute: updateMyProfileExecute,
    });

    registerTool({
      name: "bamdra_user_bind_refresh_my_binding",
      description:
        "Refresh the current user's identity binding when the session looks unresolved, stale, or mapped to the wrong external identity, then fetch the profile again",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          sessionId: { type: "string" },
        },
      },
      execute: refreshMyBindingExecute,
    });

    registerTool({
      name: SELF_TOOL_ALIASES[0],
      description: "Alias of bamdra_user_bind_get_my_profile",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          sessionId: { type: "string" },
        },
      },
      execute: getMyProfileExecute,
    });

    registerTool({
      name: SELF_TOOL_ALIASES[1],
      description: "Alias of bamdra_user_bind_update_my_profile",
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
      execute: updateMyProfileExecute,
    });

    registerTool({
      name: SELF_TOOL_ALIASES[2],
      description: "Alias of bamdra_user_bind_refresh_my_binding",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          sessionId: { type: "string" },
        },
      },
      execute: refreshMyBindingExecute,
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

    const contactScopeBlockedAccounts = new Set<string>();
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
        if (looksLikeFeishuContactScopeError(message)) {
          contactScopeBlockedAccounts.add(account.accountId);
        }
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

    if (contactScopeBlockedAccounts.size > 0) {
      return {
        userId: "",
        source: "feishu-contact-scope-missing",
        profilePatch: {
          notes: renderFeishuContactScopeGuidance([...contactScopeBlockedAccounts]),
        },
      };
    }

    logUserBindEvent("feishu-resolution-empty", { openId });
    return null;
  }

  private async ensureFeishuScopeStatus(account?: FeishuAccountCredentials): Promise<FeishuScopeStatus> {
    if (account) {
      const cached = this.feishuScopeStatusCache.get(account.accountId);
      if (cached) {
        return cached;
      }
      try {
        const token = await this.getFeishuAppAccessToken(account);
        const result = await feishuJsonRequest(
          account,
          "/open-apis/application/v6/scopes",
          token,
        );
        const scopes = extractScopes(result);
        const status: FeishuScopeStatus = {
          scopes,
          missingIdentityScopes: REQUIRED_FEISHU_IDENTITY_SCOPES.filter((scope) => !scopes.includes(scope)),
          hasDocumentAccess: scopes.some((scope) => scope.startsWith("bitable:") || scope.startsWith("drive:") || scope.startsWith("docx:") || scope.startsWith("docs:")),
        };
        this.feishuScopeStatusCache.set(account.accountId, status);
        logUserBindEvent("feishu-scopes-read", {
          accountId: account.accountId,
          ...status,
        });
        return status;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        logUserBindEvent("feishu-scopes-attempt-failed", { accountId: account.accountId, message });
      }
    }

    const accounts = readFeishuAccountsFromOpenClawConfig();
    for (const candidate of accounts) {
      const cached = this.feishuScopeStatusCache.get(candidate.accountId);
      if (cached?.hasDocumentAccess) {
        return cached;
      }
    }
    for (const candidate of accounts) {
      const status = await this.ensureFeishuScopeStatus(candidate);
      if (status.hasDocumentAccess) {
        return status;
      }
    }

    const executor = this.host.callTool ?? this.host.invokeTool;
    if (typeof executor === "function") {
      try {
        const result = await executor.call(this.host, "feishu_app_scopes", {}) as Record<string, unknown>;
        const scopes = extractScopes(result);
        const status = {
          scopes,
          missingIdentityScopes: REQUIRED_FEISHU_IDENTITY_SCOPES.filter((scope) => !scopes.includes(scope)),
          hasDocumentAccess: scopes.some((scope) => scope.startsWith("bitable:") || scope.startsWith("drive:") || scope.startsWith("docx:") || scope.startsWith("docs:")),
        };
        logUserBindEvent("feishu-scopes-read", status);
        return status;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        logUserBindEvent("feishu-scopes-failed", { message });
        this.store.recordIssue("feishu-scope-read", message);
      }
    }

    return {
      scopes: [],
      missingIdentityScopes: [...REQUIRED_FEISHU_IDENTITY_SCOPES],
      hasDocumentAccess: false,
    };
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
    async runPendingBindingSweep() {
      return runtime.runPendingBindingSweep();
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
  let changed = false;
  for (const item of iterAgentConfigs(agents)) {
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
  let changed = false;
  let attached = false;
  for (const item of iterAgentConfigs(agents)) {
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
  const list = Array.isArray(agents.list) ? agents.list : [];
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

function *iterAgentConfigs(agents: Record<string, unknown>): Iterable<Record<string, unknown>> {
  const seen = new Set<Record<string, unknown>>();
  const list = Array.isArray(agents.list) ? agents.list : [];
  for (const item of list) {
    if (item && typeof item === "object") {
      const agent = item as Record<string, unknown>;
      seen.add(agent);
      yield agent;
    }
  }
  if (list.length > 0) {
    return;
  }
  for (const [key, value] of Object.entries(agents)) {
    if (key === "list" || key === "defaults" || !value || typeof value !== "object" || Array.isArray(value)) {
      continue;
    }
    const agent = value as Record<string, unknown>;
    if (seen.has(agent)) {
      continue;
    }
    seen.add(agent);
    if (!getConfiguredAgentId(agent)) {
      agent.id = key;
    }
    yield agent;
  }
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
  const senderIdFromText = extractSenderIdFromMetadataText(metadataText);
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
    ?? inferChannelTypeFromSenderId(extractSenderId(record, sender, senderInfo, conversationInfo, message, senderIdFromText))
    ?? inferChannelTypeFromSessionId(sessionId);
  const openId = extractSenderId(record, sender, senderInfo, conversationInfo, message, senderIdFromText)
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

function enrichIdentityContext(context: unknown): Record<string, unknown> {
  const record = (context && typeof context === "object") ? { ...(context as Record<string, unknown>) } : {};
  const sessionManager = record.sessionManager && typeof record.sessionManager === "object"
    ? record.sessionManager
    : null;
  if (!sessionManager) {
    return record;
  }
  const sessionSnapshot = readSessionManagerSnapshot(sessionManager);
  if (!sessionSnapshot) {
    return record;
  }
  if (!record.sessionId && sessionSnapshot.sessionId) {
    record.sessionId = sessionSnapshot.sessionId;
  }
  if (!record.text && sessionSnapshot.metadataText) {
    record.text = sessionSnapshot.metadataText;
  }
  const metadata = record.metadata && typeof record.metadata === "object"
    ? { ...(record.metadata as Record<string, unknown>) }
    : {};
  if (!metadata.text && sessionSnapshot.metadataText) {
    metadata.text = sessionSnapshot.metadataText;
  }
  if (!metadata.sessionId && sessionSnapshot.sessionId) {
    metadata.sessionId = sessionSnapshot.sessionId;
  }
  if (Object.keys(metadata).length > 0) {
    record.metadata = metadata;
  }
  if (!record.channelType && sessionSnapshot.channelType) {
    record.channelType = sessionSnapshot.channelType;
  }
  if (!record.openId && sessionSnapshot.openId) {
    record.openId = sessionSnapshot.openId;
  }
  return record;
}

function readSessionManagerSnapshot(sessionManager: Record<string, unknown>): {
  sessionId: string | null;
  metadataText: string | null;
  channelType: string | null;
  openId: string | null;
} | null {
  try {
    const getSessionId = (sessionManager as { getSessionId?: () => unknown }).getSessionId;
    const getBranch = (sessionManager as { getBranch?: () => unknown }).getBranch;
    const sessionId = typeof getSessionId === "function" ? asNullableString(getSessionId()) : null;
    const branch = typeof getBranch === "function" ? getBranch() : [];
    if (!Array.isArray(branch) || branch.length === 0) {
      return sessionId ? {
        sessionId,
        metadataText: null,
        channelType: inferChannelTypeFromSessionId(sessionId),
        openId: extractOpenIdFromSessionId(sessionId),
      } : null;
    }
    for (let i = branch.length - 1; i >= 0; i -= 1) {
      const entry = branch[i];
      if (!entry || typeof entry !== "object") {
        continue;
      }
      const item = entry as Record<string, unknown>;
      const message = (item.message && typeof item.message === "object")
        ? item.message as Record<string, unknown>
        : null;
      if (item.type !== "message" || !message || message.role !== "user") {
        continue;
      }
      const metadataText = extractMessageText(message);
      if (!metadataText || !looksLikeIdentityMetadata(metadataText)) {
        continue;
      }
      const openId = extractSenderIdFromMetadataText(metadataText);
      const channelType = inferChannelTypeFromSenderId(openId) ?? inferChannelTypeFromSessionId(sessionId);
      return {
        sessionId,
        metadataText,
        channelType,
        openId,
      };
    }
    return sessionId ? {
      sessionId,
      metadataText: null,
      channelType: inferChannelTypeFromSessionId(sessionId),
      openId: extractOpenIdFromSessionId(sessionId),
    } : null;
  } catch (error) {
    logUserBindEvent("session-manager-read-failed", {
      error: error instanceof Error ? error.message : String(error),
    });
    return null;
  }
}

function extractMessageText(message: Record<string, unknown>): string | null {
  const content = message.content;
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    return null;
  }
  const text = content
    .filter((part) => part && typeof part === "object" && (part as Record<string, unknown>).type === "text" && typeof (part as Record<string, unknown>).text === "string")
    .map((part) => String((part as Record<string, unknown>).text))
    .join("\n");
  return text || null;
}

function looksLikeIdentityMetadata(text: string): boolean {
  return text.includes("Conversation info (untrusted metadata)")
    || text.includes("Sender (untrusted metadata)")
    || /"sender_id"\s*:\s*"/.test(text);
}

function extractUserUtterance(context: unknown): string | null {
  const record = (context && typeof context === "object") ? context as Record<string, unknown> : {};
  const rawText = extractHookContextText(record)
    ?? readLatestUserMessageFromSessionManager(record.sessionManager);
  if (!rawText) {
    return null;
  }
  const stripped = stripIdentityMetadata(rawText);
  if (!stripped) {
    return null;
  }
  if (looksLikeIdentityMetadata(stripped)) {
    return null;
  }
  return stripped;
}

function extractHookContextText(record: Record<string, unknown>): string | null {
  const directText = normalizeHookText(
    record.bodyForAgent
    ?? record.body
    ?? record.prompt
    ?? record.text
    ?? findNestedValue(record, ["context", "bodyForAgent"])
    ?? findNestedValue(record, ["context", "body"])
    ?? findNestedValue(record, ["context", "text"])
    ?? findNestedValue(record, ["context", "content"]),
  );
  if (directText) {
    return directText;
  }

  const message = findNestedRecord(record, ["message"], ["event", "message"], ["payload", "message"]);
  const messageText = extractMessageText(message);
  if (messageText) {
    return messageText;
  }

  const inputText = extractTextFromInput(record.input);
  if (inputText) {
    return inputText;
  }

  const messages = Array.isArray(record.messages) ? record.messages : [];
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const item = messages[i];
    if (!item || typeof item !== "object") {
      continue;
    }
    const messageRecord = item as Record<string, unknown>;
    const role = asNullableString(messageRecord.role) ?? "user";
    if (role !== "user") {
      continue;
    }
    const text = normalizeHookText(messageRecord.text ?? messageRecord.content);
    if (text) {
      return text;
    }
  }

  return null;
}

function extractTextFromInput(input: unknown): string | null {
  if (typeof input === "string") {
    return normalizeHookText(input);
  }
  if (!input || typeof input !== "object") {
    return null;
  }
  const record = input as Record<string, unknown>;
  const directText = normalizeHookText(record.text ?? record.content);
  if (directText) {
    return directText;
  }
  const message = (record.message && typeof record.message === "object")
    ? record.message as Record<string, unknown>
    : null;
  const messageText = normalizeHookText(message?.text ?? message?.content);
  if (messageText) {
    return messageText;
  }
  const messages = Array.isArray(record.messages) ? record.messages : [];
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const item = messages[i];
    if (!item || typeof item !== "object") {
      continue;
    }
    const messageRecord = item as Record<string, unknown>;
    const role = asNullableString(messageRecord.role) ?? "user";
    if (role !== "user") {
      continue;
    }
    const text = normalizeHookText(messageRecord.text ?? messageRecord.content);
    if (text) {
      return text;
    }
  }
  return null;
}

function normalizeHookText(value: unknown): string | null {
  if (typeof value === "string") {
    return value.trim() || null;
  }
  if (Array.isArray(value)) {
    const text = value
      .map((item) => {
        if (!item || typeof item !== "object") {
          return "";
        }
        return asNullableString((item as Record<string, unknown>).text) ?? "";
      })
      .filter(Boolean)
      .join("\n")
      .trim();
    return text || null;
  }
  return null;
}

function readLatestUserMessageFromSessionManager(sessionManager: unknown): string | null {
  if (!sessionManager || typeof sessionManager !== "object") {
    return null;
  }
  try {
    const getBranch = (sessionManager as { getBranch?: () => unknown }).getBranch;
    const branch = typeof getBranch === "function" ? getBranch() : [];
    if (!Array.isArray(branch) || branch.length === 0) {
      return null;
    }
    for (let i = branch.length - 1; i >= 0; i -= 1) {
      const entry = branch[i];
      if (!entry || typeof entry !== "object") {
        continue;
      }
      const item = entry as Record<string, unknown>;
      const message = (item.message && typeof item.message === "object")
        ? item.message as Record<string, unknown>
        : null;
      if (item.type !== "message" || !message || message.role !== "user") {
        continue;
      }
      const text = extractMessageText(message);
      if (text && !looksLikeIdentityMetadata(text)) {
        return text.trim();
      }
    }
  } catch (error) {
    logUserBindEvent("session-manager-user-message-read-failed", {
      error: error instanceof Error ? error.message : String(error),
    });
  }
  return null;
}

function mergeSemanticCaptureContext(event: unknown, context: unknown): Record<string, unknown> {
  const merged: Record<string, unknown> = {};
  if (event && typeof event === "object") {
    Object.assign(merged, event as Record<string, unknown>);
  }
  if (context && typeof context === "object") {
    const record = context as Record<string, unknown>;
    for (const [key, value] of Object.entries(record)) {
      if (merged[key] === undefined) {
        merged[key] = value;
      }
    }
  }
  if (merged.sessionManager === undefined && context && typeof context === "object") {
    merged.sessionManager = (context as Record<string, unknown>).sessionManager;
  }
  return merged;
}

function stripIdentityMetadata(text: string): string | null {
  const hadMetadata = looksLikeIdentityMetadata(text);
  let cleaned = text
    .replace(/Conversation info \(untrusted metadata\):\s*```json[\s\S]*?```/gi, "")
    .replace(/Sender \(untrusted metadata\):\s*```json[\s\S]*?```/gi, "")
    .replace(/^\s*\[[^\n]*message_id[^\n]*\]\s*$/gim, "")
    .replace(/^\s*\[[^\n]*sender_id[^\n]*\]\s*$/gim, "");
  if (hadMetadata) {
    cleaned = cleaned.replace(/^\s*[^\n:：]{1,40}[:：]\s*/m, "");
  }
  cleaned = cleaned.trim();
  return cleaned || null;
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
  if (sessionId.includes(":discord:")) {
    return "discord";
  }
  if (sessionId.includes(":googlechat:")) {
    return "googlechat";
  }
  if (sessionId.includes(":slack:")) {
    return "slack";
  }
  if (sessionId.includes(":mattermost:")) {
    return "mattermost";
  }
  if (sessionId.includes(":signal:")) {
    return "signal";
  }
  if (sessionId.includes(":imessage:")) {
    return "imessage";
  }
  if (sessionId.includes(":msteams:")) {
    return "msteams";
  }
  return null;
}

function inferChannelTypeFromSenderId(senderId: string | null): string | null {
  if (!senderId) {
    return null;
  }
  if (/^(?:ou|oc)_[A-Za-z0-9_-]+$/.test(senderId)) {
    return "feishu";
  }
  if (/@(?:s\.whatsapp\.net|g\.us)$/.test(senderId)) {
    return "whatsapp";
  }
  if (/^users\/.+/.test(senderId) || /^spaces\/.+/.test(senderId)) {
    return "googlechat";
  }
  return null;
}

function extractSenderId(
  record: Record<string, unknown>,
  sender: Record<string, unknown>,
  senderInfo: Record<string, unknown> | null,
  conversationInfo: Record<string, unknown> | null,
  message: Record<string, unknown>,
  senderIdFromText: string | null,
): string | null {
  return firstNonEmptyString(
    asNullableString(record.openId),
    asNullableString(record.senderId),
    asNullableString(record.userId),
    asNullableString(record.fromId),
    asNullableString(record.participantId),
    asNullableString(record.authorId),
    asNullableString(sender.id),
    asNullableString(sender.open_id),
    asNullableString(sender.openId),
    asNullableString(sender.user_id),
    asNullableString(sender.userId),
    asNullableString(sender.sender_id),
    asNullableString(sender.senderId),
    asNullableString(sender.from_id),
    asNullableString(sender.fromId),
    asNullableString(sender.author_id),
    asNullableString(sender.authorId),
    asNullableString(sender.chat_id),
    asNullableString(sender.chatId),
    asNullableString(sender.participant),
    asNullableString(sender.participant_id),
    asNullableString(sender.participantId),
    asNullableString(sender.jid),
    asNullableString(sender.handle),
    asNullableString(sender.username),
    asNullableString(sender.phone),
    asNullableString(sender.phone_number),
    asNullableString(findNestedValue(sender, ["from", "id"])),
    asNullableString(findNestedValue(sender, ["author", "id"])),
    asNullableString(findNestedValue(message, ["from", "id"])),
    asNullableString(findNestedValue(message, ["author", "id"])),
    asNullableString(findNestedValue(message, ["user", "id"])),
    asNullableString(senderInfo?.id),
    asNullableString(senderInfo?.user_id),
    asNullableString(senderInfo?.sender_id),
    asNullableString(conversationInfo?.sender_id),
    asNullableString(conversationInfo?.user_id),
    asNullableString(conversationInfo?.from_id),
    senderIdFromText,
  );
}

function extractSenderIdFromMetadataText(metadataText: string | null): string | null {
  if (!metadataText) {
    return null;
  }
  return firstNonEmptyString(
    extractRegexValue(metadataText, /"sender_id"\s*:\s*"([^"]+)"/),
    extractRegexValue(metadataText, /"user_id"\s*:\s*"([^"]+)"/),
    extractRegexValue(metadataText, /"from_id"\s*:\s*"([^"]+)"/),
    extractRegexValue(metadataText, /"author_id"\s*:\s*"([^"]+)"/),
    extractRegexValue(metadataText, /"chat_id"\s*:\s*"([^"]+)"/),
    extractRegexValue(metadataText, /"participant"\s*:\s*"([^"]+)"/),
    extractRegexValue(metadataText, /"jid"\s*:\s*"([^"]+)"/),
    extractRegexValue(metadataText, /"id"\s*:\s*"((?:ou|oc)_[^"]+)"/),
  );
}

function firstNonEmptyString(...values: Array<string | null | undefined>): string | null {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
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
  for (const channel of ["feishu", "telegram", "whatsapp", "discord", "googlechat", "slack", "mattermost", "signal", "imessage", "msteams"]) {
    const marker = `:${channel}:`;
    const channelIndex = sessionId.indexOf(marker);
    if (channelIndex < 0) {
      continue;
    }
    const remainder = sessionId.slice(channelIndex + marker.length);
    const modeSeparator = remainder.indexOf(":");
    if (modeSeparator < 0) {
      continue;
    }
    const senderId = remainder.slice(modeSeparator + 1).trim();
    if (senderId) {
      return senderId;
    }
  }
  const match = sessionId.match(/:([^:]+)$/);
  return match?.[1]?.trim() || null;
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

function extractProfilePatchOperations(params: Record<string, unknown>): ProfilePatchOperations | undefined {
  const operations: ProfilePatchOperations = {};
  const fields = ["nickname", "preferences", "personality", "role", "timezone", "notes"] as const;
  for (const field of fields) {
    const raw = asNullableString(params[`${field}Operation`]) ?? asNullableString(params[`${field}_operation`]);
    if (raw === "replace" || raw === "append" || raw === "remove") {
      operations[field] = raw;
    }
  }
  return Object.keys(operations).length > 0 ? operations : undefined;
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
  const notes = sanitizeProfileNotes(profile.notes) ?? defaultProfileNotes();
  const frontmatterLines = [
    "---",
    `userId: ${escapeFrontmatter(profile.userId)}`,
  ];
  if (profile.name) {
    frontmatterLines.push(`name: ${escapeFrontmatter(profile.name)}`);
  }
  if (profile.nickname) {
    frontmatterLines.push(`nickname: ${escapeFrontmatter(profile.nickname)}`);
  }
  if (profile.timezone) {
    frontmatterLines.push(`timezone: ${escapeFrontmatter(profile.timezone)}`);
  }
  if (profile.preferences) {
    frontmatterLines.push(`preferences: ${escapeFrontmatter(profile.preferences)}`);
  }
  if (profile.personality) {
    frontmatterLines.push(`personality: ${escapeFrontmatter(profile.personality)}`);
  }
  if (profile.role) {
    frontmatterLines.push(`role: ${escapeFrontmatter(profile.role)}`);
  }
  frontmatterLines.push(
    `visibility: ${escapeFrontmatter(profile.visibility)}`,
    `source: ${escapeFrontmatter(profile.source)}`,
    `updatedAt: ${escapeFrontmatter(profile.updatedAt)}`,
    `syncHash: ${escapeFrontmatter(computeProfilePayloadHash({
      name: profile.name,
      nickname: profile.nickname,
      timezone: profile.timezone,
      preferences: profile.preferences,
      personality: profile.personality,
      role: profile.role,
      visibility: profile.visibility,
    }, notes))}`,
    "---",
  );
  const frontmatter = frontmatterLines.join("\n");
  const confirmedProfileLines = renderConfirmedProfileSection(profile);
  return `${frontmatter}

# 用户画像

这个文件由系统维护。Frontmatter 是系统机器读写的结构化源；下面的“已确认画像”是给人看的同步镜像。没有出现的字段，表示当前还没有确认，不代表空值结论。

## 已确认画像

${confirmedProfileLines}

## 补充备注

${notes}
`;
}

function renderConfirmedProfileSection(profile: UserProfile): string {
  const rows: string[] = [];
  if (profile.name) {
    rows.push(`- 姓名：${profile.name}`);
  }
  if (profile.nickname) {
    rows.push(`- 称呼：${profile.nickname}`);
  }
  if (profile.preferences) {
    rows.push(`- 回答偏好：${profile.preferences}`);
  }
  if (profile.personality) {
    rows.push(`- 风格偏好：${profile.personality}`);
  }
  if (profile.role) {
    rows.push(`- 角色身份：${profile.role}`);
  }
  if (profile.timezone) {
    rows.push(`- 时区：${profile.timezone}`);
  }
  return rows.length > 0
    ? rows.join("\n")
    : "- 暂无已确认的结构化画像字段";
}

function computeProfilePayloadHash(
  patch: Pick<Partial<UserProfile>, "name" | "nickname" | "timezone" | "preferences" | "personality" | "role" | "visibility">,
  notes: string | null,
): string {
  return hashId(JSON.stringify({
    name: patch.name ?? null,
    nickname: patch.nickname ?? null,
    timezone: patch.timezone ?? null,
    preferences: patch.preferences ?? null,
    personality: patch.personality ?? null,
    role: patch.role ?? null,
    visibility: patch.visibility ?? "private",
    notes: sanitizeProfileNotes(notes) ?? null,
  }));
}

function parseProfileMarkdown(markdown: string): ParsedMarkdownProfile {
  const lines = markdown.split(/\r?\n/);
  const patch: Partial<UserProfile> = {};
  let notes: string | null = null;
  let updatedAt: string | null = null;
  let source: string | null = null;
  let syncHash: string | null = null;
  let index = 0;
  if (lines[index] === "---") {
    index += 1;
    while (index < lines.length && lines[index] !== "---") {
      const line = lines[index];
      const separatorIndex = line.indexOf(":");
      if (separatorIndex > 0) {
        const key = line.slice(0, separatorIndex).trim();
        const value = line.slice(separatorIndex + 1).trim();
        if (key === "updatedAt") {
          updatedAt = value === "null" ? null : value;
        } else if (key === "source") {
          source = value === "null" ? null : value;
        } else if (key === "syncHash") {
          syncHash = value === "null" ? null : value;
        } else {
          applyFrontmatterField(patch, key, value);
        }
      }
      index += 1;
    }
    if (lines[index] === "---") {
      index += 1;
    }
  }
  const body = lines.slice(index).join("\n");
  const notesMatch = body.match(/##\s*(?:补充备注|备注)\s*\n([\s\S]*)$/);
  if (notesMatch?.[1]) {
    notes = sanitizeProfileNotes(notesMatch[1]);
  }
  return {
    profilePatch: patch,
    notes,
    updatedAt,
    source,
    syncHash,
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

async function inferSemanticProfileExtraction(
  messageText: string,
  currentProfile: UserProfile,
): Promise<SemanticProfileExtraction | null> {
  const model = readProfileExtractorModelFromOpenClawConfig();
  if (!model) {
    return null;
  }
  const response = await fetch(`${model.baseUrl}/chat/completions`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${model.apiKey}`,
      "content-type": "application/json; charset=utf-8",
    },
    body: JSON.stringify({
      model: model.modelId,
      temperature: 0,
      max_tokens: 400,
      response_format: { type: "json_object" },
      messages: [
        {
          role: "system",
          content: [
            "You extract stable user-profile information from a single user message.",
            "Return JSON only.",
            "Only capture durable, reusable traits or preferences that should affect future conversations.",
            "Ignore transient task requirements, one-off requests, or speculative guesses.",
            "Prefer precision, but do not miss clear self-descriptions or clear addressing / style preferences.",
            "Allowed fields: nickname, preferences, personality, role, timezone, notes.",
            "For each field, also decide the update operation: replace, append, or remove.",
            "notes is only for durable boundaries or habits that do not fit the structured fields.",
            "A preferred form of address counts as nickname even when phrased naturally, for example: '以后叫我丰哥', '叫我亲爱的老大', '你就叫我阿丰'.",
            "A reusable response preference counts as preferences, for example: '先给结论再展开', '说话直接一点但别太硬', '别太官方'.",
            "Use append when the user adds another stable preference without revoking the old one.",
            "Use replace when the user corrects or changes an existing stable preference.",
            "Use remove when the user clearly asks to drop a specific old preference or trait.",
            "If a message clearly contains both an addressing preference and a communication preference, capture both.",
            "Do not require rigid trigger phrases. Judge by meaning.",
            "Do not copy placeholders, examples, or template language.",
            'Return exactly this shape: {"should_update":boolean,"confidence":number,"patch":{"nickname":string?,"preferences":string?,"personality":string?,"role":string?,"timezone":string?,"notes":string?},"operations":{"nickname":"replace|append|remove"?,"preferences":"replace|append|remove"?,"personality":"replace|append|remove"?,"role":"replace|append|remove"?,"timezone":"replace|append|remove"?,"notes":"replace|append|remove"?}}',
          ].join("\n"),
        },
        {
          role: "user",
          content: JSON.stringify({
            current_profile: {
              nickname: currentProfile.nickname,
              preferences: currentProfile.preferences,
              personality: currentProfile.personality,
              role: currentProfile.role,
              timezone: currentProfile.timezone,
              notes: currentProfile.notes,
            },
            latest_user_message: messageText,
          }),
        },
      ],
    }),
  });
  const payload = await response.json() as Record<string, unknown>;
  if (!response.ok) {
    throw new Error(`semantic profile extractor request failed: ${JSON.stringify(payload)}`);
  }
  const content = extractOpenAiMessageContent(payload);
  const parsed = parseSemanticExtractionResult(content);
  if (!parsed) {
    throw new Error(`semantic profile extractor returned unreadable content: ${content}`);
  }
  return parsed;
}

function extractOpenAiMessageContent(payload: Record<string, unknown>): string {
  const choices = Array.isArray(payload.choices) ? payload.choices : [];
  const first = choices[0];
  if (!first || typeof first !== "object") {
    return "";
  }
  const message = (first as Record<string, unknown>).message;
  if (!message || typeof message !== "object") {
    return "";
  }
  const content = (message as Record<string, unknown>).content;
  if (typeof content === "string") {
    return content.trim();
  }
  if (!Array.isArray(content)) {
    return "";
  }
  return content
    .map((item) => {
      if (!item || typeof item !== "object") {
        return "";
      }
      const record = item as Record<string, unknown>;
      return asNullableString(record.text) ?? "";
    })
    .filter(Boolean)
    .join("\n")
    .trim();
}

function parseSemanticExtractionResult(content: string): SemanticProfileExtraction | null {
  const jsonText = extractJsonObject(content);
  if (!jsonText) {
    return null;
  }
  try {
    const parsed = JSON.parse(jsonText) as Record<string, unknown>;
    const patchInput = (parsed.patch && typeof parsed.patch === "object")
      ? parsed.patch as Record<string, unknown>
      : {};
    const operationsInput = (parsed.operations && typeof parsed.operations === "object")
      ? parsed.operations as Record<string, unknown>
      : {};
    return {
      shouldUpdate: parsed.should_update === true || parsed.shouldUpdate === true,
      confidence: Number(parsed.confidence ?? 0),
      patch: {
        nickname: asNullableString(patchInput.nickname),
        preferences: asNullableString(patchInput.preferences),
        personality: asNullableString(patchInput.personality),
        role: asNullableString(patchInput.role),
        timezone: asNullableString(patchInput.timezone),
        notes: asNullableString(patchInput.notes),
      },
      operations: parseProfilePatchOperations(operationsInput),
    };
  } catch {
    return null;
  }
}

function parseProfilePatchOperations(input: Record<string, unknown>): ProfilePatchOperations | undefined {
  const operations: ProfilePatchOperations = {};
  for (const field of ["nickname", "preferences", "personality", "role", "timezone", "notes"] as const) {
    const raw = asNullableString(input[field]);
    if (raw === "replace" || raw === "append" || raw === "remove") {
      operations[field] = raw;
    }
  }
  return Object.keys(operations).length > 0 ? operations : undefined;
}

function extractJsonObject(content: string): string | null {
  const trimmed = content.trim();
  if (!trimmed) {
    return null;
  }
  const fenced = trimmed.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
  if (fenced?.[1]) {
    return fenced[1].trim();
  }
  const start = trimmed.indexOf("{");
  const end = trimmed.lastIndexOf("}");
  if (start >= 0 && end > start) {
    return trimmed.slice(start, end + 1);
  }
  return null;
}

function cleanupSemanticProfilePatch(
  patch: Partial<UserProfile>,
  currentProfile: UserProfile,
  operations?: ProfilePatchOperations,
): { patch: Partial<UserProfile>; operations?: ProfilePatchOperations } {
  const next: Partial<UserProfile> = {};
  const nextOperations: ProfilePatchOperations = {};
  const nickname = asNullableString(patch.nickname);
  const preferences = asNullableString(patch.preferences);
  const personality = asNullableString(patch.personality);
  const role = asNullableString(patch.role);
  const timezone = asNullableString(patch.timezone);
  const notes = asNullableString(patch.notes);

  if (nickname && nickname !== currentProfile.nickname) {
    next.nickname = nickname;
    if (operations?.nickname) {
      nextOperations.nickname = operations.nickname;
    }
  }
  if (preferences && preferences !== currentProfile.preferences) {
    next.preferences = preferences;
    if (operations?.preferences) {
      nextOperations.preferences = operations.preferences;
    }
  }
  if (personality && personality !== currentProfile.personality) {
    next.personality = personality;
    if (operations?.personality) {
      nextOperations.personality = operations.personality;
    }
  }
  if (role && role !== currentProfile.role) {
    next.role = role;
    if (operations?.role) {
      nextOperations.role = operations.role;
    }
  }
  if (timezone && timezone !== currentProfile.timezone) {
    next.timezone = timezone;
    if (operations?.timezone) {
      nextOperations.timezone = operations.timezone;
    }
  }
  if (notes) {
    next.notes = notes;
    if (operations?.notes) {
      nextOperations.notes = operations.notes;
    }
  }
  return {
    patch: next,
    operations: Object.keys(nextOperations).length > 0 ? nextOperations : undefined,
  };
}

function applyProfilePatchOperations(
  currentProfile: UserProfile,
  patch: Partial<UserProfile>,
  operations?: ProfilePatchOperations,
): Partial<UserProfile> {
  const next: Partial<UserProfile> = {};
  const fields = ["nickname", "preferences", "personality", "role", "timezone", "notes"] as const;
  for (const field of fields) {
    if (patch[field] === undefined) {
      continue;
    }
    const currentValue = currentProfile[field];
    const incomingValue = asNullableString(patch[field]);
    const operation = operations?.[field] ?? defaultProfileFieldOperation(field);
    const resolved = resolveProfileFieldUpdate(field, currentValue, incomingValue, operation);
    if (resolved !== undefined) {
      next[field] = resolved;
    }
  }
  return next;
}

function defaultProfileFieldOperation(field: keyof ProfilePatchOperations): ProfileFieldOperation {
  if (field === "notes") {
    return "append";
  }
  return "replace";
}

function resolveProfileFieldUpdate(
  field: keyof ProfilePatchOperations,
  currentValue: string | null,
  incomingValue: string | null,
  operation: ProfileFieldOperation,
): string | null | undefined {
  if (operation === "remove") {
    if (!currentValue) {
      return undefined;
    }
    if (!incomingValue) {
      return null;
    }
    const removed = removeProfileFeatureValue(currentValue, incomingValue);
    return removed === currentValue ? undefined : removed;
  }
  if (!incomingValue) {
    return undefined;
  }
  if (operation === "append") {
    if (field === "notes") {
      const mergedNotes = joinNotes(currentValue, incomingValue);
      return mergedNotes === currentValue ? undefined : mergedNotes;
    }
    const appended = appendProfileFeatureValue(currentValue, incomingValue);
    return appended === currentValue ? undefined : appended;
  }
  return incomingValue === currentValue ? undefined : incomingValue;
}

function appendProfileFeatureValue(currentValue: string | null, incomingValue: string): string {
  if (!currentValue) {
    return incomingValue;
  }
  const currentItems = splitProfileFeatureValues(currentValue);
  const incomingItems = splitProfileFeatureValues(incomingValue);
  const merged = [...currentItems];
  for (const item of incomingItems) {
    if (!merged.includes(item)) {
      merged.push(item);
    }
  }
  return merged.join("；");
}

function removeProfileFeatureValue(currentValue: string, incomingValue: string): string | null {
  const currentItems = splitProfileFeatureValues(currentValue);
  const removals = new Set(splitProfileFeatureValues(incomingValue));
  const nextItems = currentItems.filter((item) => !removals.has(item));
  if (nextItems.length === currentItems.length) {
    return currentValue;
  }
  return nextItems.length > 0 ? nextItems.join("；") : null;
}

function splitProfileFeatureValues(value: string): string[] {
  return value
    .split(/[\n;；,，、]+/u)
    .map((item) => item.trim())
    .filter(Boolean);
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

function readProfileExtractorModelFromOpenClawConfig(): OpenAiCompatibleModelConfig | null {
  const openclawConfigPath = join(homedir(), ".openclaw", "openclaw.json");
  if (!existsSync(openclawConfigPath)) {
    return null;
  }
  try {
    const parsed = JSON.parse(readFileSync(openclawConfigPath, "utf8")) as Record<string, unknown>;
    const models = (parsed.models && typeof parsed.models === "object")
      ? parsed.models as Record<string, unknown>
      : {};
    const providers = (models.providers && typeof models.providers === "object")
      ? models.providers as Record<string, unknown>
      : {};
    const agents = (parsed.agents && typeof parsed.agents === "object")
      ? parsed.agents as Record<string, unknown>
      : {};
    const defaults = (agents.defaults && typeof agents.defaults === "object")
      ? agents.defaults as Record<string, unknown>
      : {};
    const defaultModel = (defaults.model && typeof defaults.model === "object")
      ? defaults.model as Record<string, unknown>
      : {};
    const configuredModel = asNullableString(defaultModel.primary) ?? asNullableString(models.primary);
    if (!configuredModel || !configuredModel.includes("/")) {
      return null;
    }
    const [providerId, modelId] = configuredModel.split("/", 2);
    const provider = (providers[providerId] && typeof providers[providerId] === "object")
      ? providers[providerId] as Record<string, unknown>
      : null;
    if (!provider) {
      return null;
    }
    const api = asNullableString(provider.api);
    const baseUrl = asNullableString(provider.baseUrl) ?? asNullableString(provider.baseURL);
    const apiKey = asNullableString(provider.apiKey);
    if (api !== "openai-completions" || !baseUrl || !apiKey) {
      return null;
    }
    return {
      providerId,
      modelId,
      baseUrl: baseUrl.replace(/\/+$/, ""),
      apiKey,
    };
  } catch (error) {
    logUserBindEvent("profile-extractor-config-read-failed", {
      message: error instanceof Error ? error.message : String(error),
    });
    return null;
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
    "- 这里只记录已经确认的长期画像信息，不记录临时任务要求。",
    `- 若用户尚未明确提供时区，默认跟随当前服务器时区（当前检测值：${getServerTimezone()}）。`,
    "- 备注里适合写暂时还不便结构化、但已经确认的长期习惯、边界和协作偏好。",
  ].join("\n");
}

function createEphemeralUserId(channelType: string, openId: string | null, sessionId: string): string {
  return `temp_${hashId(`${channelType}:${openId ?? sessionId}`)}`;
}

function createProvisionalUserId(channelType: string, openId: string): string {
  return `${channelType}:oid_${hashId(`${channelType}:${openId}`)}`;
}

function createStableLocalUserId(channelType: string, openId: string | null, sessionId: string): string {
  return `${channelType}:ub_${hashId(`${channelType}:${openId ?? sessionId}`)}`;
}

function isProvisionalScopedUserId(userId: string): boolean {
  return /:oid_[a-f0-9]+$/i.test(userId);
}

function getPendingBindingRetryDelayMs(reason: string, attempts: number): number {
  if (reason === "feishu-contact-scope-missing") {
    return Math.min(30 * 60 * 1000, Math.max(60_000, attempts * 5 * 60_000));
  }
  return Math.min(10 * 60 * 1000, Math.max(15_000, attempts * 30_000));
}

function scopeUserId(channelType: string, userId: string): string {
  if (!userId) {
    return userId;
  }
  if (channelType === "manual" || channelType === "local") {
    return userId;
  }
  if (userId.startsWith("temp_") || userId.startsWith("ub_")) {
    return userId;
  }
  if (extractChannelFromScopedUserId(userId)) {
    return userId;
  }
  return `${channelType}:${userId}`;
}

function getExternalUserId(channelType: string, userId: string): string {
  if (!userId) {
    return userId;
  }
  if (channelType === "manual" || channelType === "local") {
    return userId;
  }
  if (userId.startsWith("temp_") || userId.startsWith("ub_")) {
    return userId;
  }
  const currentChannel = extractChannelFromScopedUserId(userId);
  if (currentChannel && userId.startsWith(`${currentChannel}:`)) {
    return userId.slice(currentChannel.length + 1);
  }
  return userId;
}

function extractChannelFromScopedUserId(userId: string): string | null {
  const match = userId.match(/^([a-z][a-z0-9_-]*):(.+)$/i);
  if (!match) {
    return null;
  }
  return match[1].toLowerCase();
}

function buildLightweightProfile(args: {
  userId: string;
  source: string;
  current: UserProfile | null;
  profilePatch: Partial<UserProfile>;
}): UserProfile {
  const now = new Date().toISOString();
  return {
    userId: args.userId,
    name: args.profilePatch.name ?? args.current?.name ?? null,
    gender: args.profilePatch.gender ?? args.current?.gender ?? null,
    email: args.profilePatch.email ?? args.current?.email ?? null,
    avatar: args.profilePatch.avatar ?? args.current?.avatar ?? null,
    nickname: args.profilePatch.nickname ?? args.current?.nickname ?? null,
    preferences: args.profilePatch.preferences ?? args.current?.preferences ?? null,
    personality: args.profilePatch.personality ?? args.current?.personality ?? null,
    role: args.profilePatch.role ?? args.current?.role ?? null,
    timezone: args.profilePatch.timezone ?? args.current?.timezone ?? getServerTimezone(),
    notes: args.profilePatch.notes ?? args.current?.notes ?? defaultProfileNotes(),
    visibility: args.profilePatch.visibility ?? args.current?.visibility ?? "private",
    source: args.source,
    updatedAt: now,
  };
}

function looksLikeFeishuContactScopeError(message: string): boolean {
  return message.includes("99991672")
    || message.includes("permission_violations")
    || message.includes("contact:contact.base:readonly")
    || message.includes("contact:contact:access_as_app")
    || message.includes("contact:contact:readonly");
}

function renderFeishuContactScopeGuidance(accountIds: string[]): string {
  const scopeText = accountIds.length > 0 ? `涉及账号：${accountIds.join("、")}。` : "";
  return [
    "当前无法完整启用用户画像。",
    "原因：对应的 Feishu App 缺少联系人权限，暂时无法稳定解析真实用户身份。",
    `${scopeText}请联系管理员开通联系人相关权限，以保证用户画像可用。`,
  ].join(" ");
}

function getServerTimezone(): string {
  try {
    const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
    if (typeof timezone === "string" && timezone.trim()) {
      return timezone.trim();
    }
  } catch {
    // ignore runtime timezone detection failure
  }
  return "UTC";
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
