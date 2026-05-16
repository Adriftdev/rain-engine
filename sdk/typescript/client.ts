/**
 * RainEngine TypeScript Client SDK
 *
 * A zero-dependency, fetch-based client for the RainEngine Gateway HTTP API.
 * Works in both browsers (native fetch) and Node.js 18+ (built-in fetch).
 *
 * @example
 * ```ts
 * import { RainEngineClient } from '@rain-engine/sdk';
 *
 * const client = new RainEngineClient('http://localhost:8080');
 * const result = await client.sendHumanInput('user1', {
 *   session_id: 'session-1',
 *   content: 'Hello, agent!',
 * });
 * console.log(result.outcome.stop_reason);
 * ```
 */

import type {
  AgentStateSnapshot,
  ApprovalIngressRequest,
  DelegationResultIngressRequest,
  EnginePolicy,
  EventIngressRequest,
  HumanInputIngressRequest,
  RuntimeRunResult,
  ScheduledWakeIngressRequest,
  WebhookIngressRequest,
  SessionRecord,
  OutcomeRecord,
  PendingApprovalRecord,
  PolicyOverlay,
  PolicyTuningRecord,
  ProfilePatchRecord,
  ReflectionRecord,
  DeliberationOutcome,
  SkillInputValidationRecord,
  PlannedSkillCall,
  SkillDefinition,
  StopReason,
  StrategyPreferenceRecord,
  ToolCallRecord,
  ToolExecutionGraph,
  ToolNodeCheckpointRecord,
  ToolNodeStatus,
  ToolPerformanceRecord,
  ToolResultRecord,
} from './rain-engine';

export type { SessionRecord, OutcomeRecord };

// ── Added Core Types (not exported by typeshare yet) ───────────────

export interface StoredSessionRecord {
  sequence_no: number;
  occurred_at_ms: number;
  session_id: string;
  trigger_id?: string;
  idempotency_key?: string;
  record: SessionRecord;
}

export interface SessionSnapshot {
  session_id: string;
  last_sequence_no?: number;
  latest_outcome?: OutcomeRecord;
  records: SessionRecord[];
}

export interface SessionSummary {
  session_id: string;
  first_recorded_at_ms: number;
  last_recorded_at_ms: number;
  record_count: number;
}

export interface RecordPage {
  session_id: string;
  next_offset?: number;
  records: StoredSessionRecord[];
}

export type SessionStatus =
  | 'empty'
  | 'running'
  | 'completed'
  | 'suspended'
  | 'delegated'
  | 'stopped'
  | 'failed';

export interface ApprovalView {
  resume_token: string;
  created_at_ms: number;
  trigger_id: string;
  step: number;
  reason: string;
  pending_calls: PlannedSkillCall[];
}

export interface ToolTimelineItem {
  call_id: string;
  skill_name: string;
  step: number;
  called_at_ms: number;
  finished_at_ms?: number;
  backend_kind: string;
  args: unknown;
  success?: boolean;
  output_preview?: string;
  failure_kind?: string;
}

export interface SelfImprovementView {
  active_overlay?: PolicyOverlay;
  reflections: ReflectionRecord[];
  policy_tunings: PolicyTuningRecord[];
  strategy_preferences: StrategyPreferenceRecord[];
  tool_performance: ToolPerformanceRecord[];
  profile_patches: ProfilePatchRecord[];
}

export interface ExecutionGraphView {
  active_graph?: ToolExecutionGraph;
  graphs: ToolExecutionGraph[];
  checkpoints: ToolNodeCheckpointRecord[];
  validations: SkillInputValidationRecord[];
  blocked_call_ids: string[];
}

export type TimelineItem =
  | { type: 'HumanInput'; payload: { actor_id: string; content: string; occurred_at_ms: number } }
  | { type: 'AssistantResponse'; payload: { content: string; stop_reason: StopReason; occurred_at_ms: number } }
  | { type: 'ToolCall'; payload: { call_id: string; skill_name: string; formatted_call: string; occurred_at_ms: number } }
  | { type: 'ToolResult'; payload: { call_id: string; skill_name: string; success: boolean; preview: string; occurred_at_ms: number } }
  | { type: 'ApprovalRequested'; payload: { resume_token: string; pending_calls: PlannedSkillCall[]; occurred_at_ms: number } }
  | { type: 'ApprovalResolved'; payload: { resume_token: string; decision: string; occurred_at_ms: number } }
  | { type: 'Plan'; payload: { summary: string; candidate_actions: string[]; confidence: number; outcome: DeliberationOutcome; occurred_at_ms: number } }
  | { type: 'ToolCheckpoint'; payload: { call_id: string; skill_name: string; status: ToolNodeStatus; attempt: number; detail?: string; occurred_at_ms: number } }
  | { type: 'ValidationFailure'; payload: { call_id: string; skill_name: string; errors: string[]; occurred_at_ms: number } }
  | { type: 'Learning'; payload: { label: string; detail: string; confidence: number; occurred_at_ms: number } }
  | { type: 'System'; payload: { label: string; detail: string; occurred_at_ms: number } };

export interface SessionView {
  session_id: string;
  status: SessionStatus;
  last_sequence_no?: number;
  latest_outcome?: OutcomeRecord;
  pending_approval?: ApprovalView;
  state: AgentStateSnapshot;
  timeline: TimelineItem[];
  tool_timeline: ToolTimelineItem[];
  self_improvement: SelfImprovementView;
  execution_graph: ExecutionGraphView;
  record_count: number;
  total_estimated_cost_usd: number;
}

export interface RuntimeCapabilities {
  version: string;
  provider_kind: string;
  default_model?: string;
  streaming: boolean;
  approvals: boolean;
  multipart_uploads: boolean;
  default_scopes: string[];
  default_policy: EnginePolicy;
  skills: SkillDefinition[];
}

export type SessionStreamEvent =
  | { type: 'session_view'; view: SessionView }
  | { type: 'records'; records: SessionRecord[] };

export interface SessionStreamHandlers {
  onView?: (view: SessionView) => void;
  onRecords?: (records: SessionRecord[]) => void;
  onError?: (event: Event) => void;
}

/** Error thrown when the gateway returns a non-2xx response. */
export class RainEngineApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly body: string,
  ) {
    super(`RainEngine API error ${status}: ${body}`);
    this.name = 'RainEngineApiError';
  }
}

/** Configuration options for the client. */
export interface RainEngineClientOptions {
  /** Additional headers to send with every request (e.g. Authorization). */
  headers?: Record<string, string>;
  /** Custom fetch implementation (defaults to global fetch). */
  fetch?: typeof globalThis.fetch;
}

/**
 * Strongly-typed async client for the RainEngine Gateway.
 *
 * Mirrors the Rust `RainEngineClient` 1:1, covering every ingress route.
 */
export class RainEngineClient {
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;
  private readonly fetchFn: typeof globalThis.fetch;

  constructor(baseUrl: string, options: RainEngineClientOptions = {}) {
    // Ensure trailing slash for reliable URL joining
    this.baseUrl = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;
    this.headers = {
      'Content-Type': 'application/json',
      ...(options.headers ?? {}),
    };
    this.fetchFn = options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  // ── Trigger: Human Input ──────────────────────────────────────────

  /**
   * Send human input to a specific actor.
   * Route: `POST /triggers/human/{actorId}`
   */
  async sendHumanInput(
    actorId: string,
    request: HumanInputIngressRequest,
  ): Promise<RuntimeRunResult> {
    return this.post(`triggers/human/${encodeURIComponent(actorId)}`, request);
  }

  // ── Trigger: Approval ─────────────────────────────────────────────

  /**
   * Submit an approval decision.
   * Route: `POST /triggers/approval`
   */
  async submitApproval(
    request: ApprovalIngressRequest,
  ): Promise<RuntimeRunResult> {
    return this.post('triggers/approval', request);
  }

  // ── Trigger: Webhook ──────────────────────────────────────────────

  /**
   * Send a webhook event from the given source.
   * Route: `POST /triggers/webhook/{source}`
   */
  async sendWebhook(
    source: string,
    request: WebhookIngressRequest,
  ): Promise<RuntimeRunResult> {
    return this.post(`triggers/webhook/${encodeURIComponent(source)}`, request);
  }

  // ── Trigger: External Event ───────────────────────────────────────

  /**
   * Send an external event from the given source.
   * Route: `POST /triggers/external/{source}`
   */
  async sendExternalEvent(
    source: string,
    request: EventIngressRequest,
  ): Promise<RuntimeRunResult> {
    return this.post(`triggers/external/${encodeURIComponent(source)}`, request);
  }

  // ── Trigger: System Observation ───────────────────────────────────

  /**
   * Send a system observation from the given source.
   * Route: `POST /triggers/system/{source}`
   */
  async sendSystemObservation(
    source: string,
    request: EventIngressRequest,
  ): Promise<RuntimeRunResult> {
    return this.post(`triggers/system/${encodeURIComponent(source)}`, request);
  }

  // ── Trigger: Scheduled Wake ───────────────────────────────────────

  /**
   * Send a scheduled wake event.
   * Route: `POST /triggers/wake`
   */
  async sendScheduledWake(
    request: ScheduledWakeIngressRequest,
  ): Promise<RuntimeRunResult> {
    return this.post('triggers/wake', request);
  }

  // ── Trigger: Delegation Result ────────────────────────────────────

  /**
   * Send a delegation result back to the engine.
   * Route: `POST /triggers/delegation-result`
   */
  async sendDelegationResult(
    request: DelegationResultIngressRequest,
  ): Promise<RuntimeRunResult> {
    return this.post('triggers/delegation-result', request);
  }

  // ── Reads: Sessions ───────────────────────────────────────────────

  /**
   * List session summaries.
   * Route: `GET /sessions`
   */
  async listSessions(params?: {
    offset?: number;
    limit?: number;
    since_ms?: number;
    until_ms?: number;
  }): Promise<SessionSummary[]> {
    return this.get('sessions', params);
  }

  /**
   * Get a full session snapshot.
   * Route: `GET /sessions/{sessionId}`
   */
  async getSession(sessionId: string): Promise<SessionSnapshot> {
    return this.get(`sessions/${encodeURIComponent(sessionId)}`);
  }

  /**
   * List paginated session records.
   * Route: `GET /sessions/{sessionId}/records`
   */
  async listRecords(
    sessionId: string,
    params?: {
      offset?: number;
      limit?: number;
      since_ms?: number;
      until_ms?: number;
    },
  ): Promise<RecordPage> {
    return this.get(`sessions/${encodeURIComponent(sessionId)}/records`, params);
  }

  /**
   * Get the server-derived control-room projection for a session.
   * Route: `GET /sessions/{sessionId}/view`
   */
  async getSessionView(sessionId: string): Promise<SessionView> {
    return this.get(`sessions/${encodeURIComponent(sessionId)}/view`);
  }

  async getExecutionGraph(sessionId: string): Promise<ExecutionGraphView> {
    return this.get(`sessions/${encodeURIComponent(sessionId)}/execution-graph`);
  }

  /**
   * Read gateway capabilities and registered tool manifests.
   * Route: `GET /capabilities`
   */
  async getCapabilities(): Promise<RuntimeCapabilities> {
    return this.get('capabilities');
  }

  /**
   * Create an EventSource to stream records for a session in real-time.
   * Route: `GET /sessions/{sessionId}/stream`
   * Note: This returns an EventSource instance which connects immediately.
   */
  streamSession(sessionId: string): EventSource {
    const url = `${this.baseUrl}sessions/${encodeURIComponent(sessionId)}/stream`;
    return new EventSource(url);
  }

  /**
   * Subscribe to typed session stream events.
   */
  streamSessionEvents(sessionId: string, handlers: SessionStreamHandlers): EventSource {
    const source = this.streamSession(sessionId);
    source.addEventListener('session_view', (event) => {
      handlers.onView?.(JSON.parse((event as MessageEvent).data) as SessionView);
    });
    source.addEventListener('records', (event) => {
      handlers.onRecords?.(JSON.parse((event as MessageEvent).data) as SessionRecord[]);
    });
    if (handlers.onError) {
      source.addEventListener('error', handlers.onError);
    }
    return source;
  }

  // ── Internal ──────────────────────────────────────────────────────

  private async get<T>(path: string, params?: Record<string, any>): Promise<T> {
    const url = new URL(`${this.baseUrl}${path}`);
    if (params) {
      for (const [key, value] of Object.entries(params)) {
        if (value !== undefined) {
          url.searchParams.append(key, String(value));
        }
      }
    }
    const response = await this.fetchFn(url.toString(), {
      method: 'GET',
      headers: this.headers,
    });

    if (!response.ok) {
      const text = await response.text();
      throw new RainEngineApiError(response.status, text);
    }

    return response.json() as Promise<T>;
  }

  private async post<T>(path: string, body: unknown): Promise<T> {
    const url = `${this.baseUrl}${path}`;
    const response = await this.fetchFn(url, {
      method: 'POST',
      headers: this.headers,
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new RainEngineApiError(response.status, text);
    }

    return response.json() as Promise<T>;
  }
}

export function formatToolCall(call: PlannedSkillCall | ToolCallRecord): string {
  const name = 'name' in call ? call.name : call.skill_name;
  const args = normalizeJsonValue(call.args);
  if (args && typeof args === 'object' && !Array.isArray(args)) {
    const entries = Object.entries(args as Record<string, unknown>);
    if (entries.length === 0) return `${name}()`;
    const rendered = entries
      .map(([key, value]) => `${key}: ${formatCompactValue(value)}`)
      .join(', ');
    return `${name}(${rendered})`;
  }
  return `${name}(${formatCompactValue(args)})`;
}

export function extractPendingApproval(records: SessionRecord[]): PendingApprovalRecord | null {
  const resolved = new Set<string>();
  for (const record of records) {
    if (record.type === 'ApprovalResolution') {
      resolved.add((record.payload as any).resume_token);
    }
  }

  for (let index = records.length - 1; index >= 0; index -= 1) {
    const record = records[index];
    if (record.type === 'PendingApproval' && !resolved.has((record.payload as any).resume_token)) {
      return record.payload as PendingApprovalRecord;
    }
  }
  return null;
}

export function deriveSessionStatus(snapshot: SessionSnapshot): SessionStatus {
  if (snapshot.records.length === 0) return 'empty';
  const pending = extractPendingApproval(snapshot.records);
  if (pending) return 'suspended';

  const stopReason = snapshot.latest_outcome?.stop_reason;
  if (!stopReason) return 'running';
  if (stopReason === 'Responded' || stopReason === 'Yielded') return 'completed';
  if (stopReason === 'Suspended') return 'suspended';
  if (stopReason === 'Delegated') return 'delegated';
  if (stopReason === 'MaxStepsReached') return 'stopped';
  return 'failed';
}

export function toTranscriptItems(records: SessionRecord[]): TimelineItem[] {
  return records.flatMap((record): TimelineItem[] => {
    if (record.type === 'Trigger') {
      const trigger = (record.payload as any).trigger;
      if (trigger?.type === 'HumanInput') {
        return [{
          type: 'HumanInput',
          payload: {
            actor_id: trigger.payload.actor_id,
            content: trigger.payload.content,
            occurred_at_ms: Date.parse((record.payload as any).recorded_at),
          },
        }];
      }
      return [{
        type: 'System',
        payload: {
          label: String(trigger?.type ?? 'trigger'),
          detail: trigger?.payload?.source ?? trigger?.payload?.reason ?? 'event received',
          occurred_at_ms: Date.parse((record.payload as any).recorded_at),
        },
      }];
    }

    if (record.type === 'Outcome') {
      const outcome = record.payload as OutcomeRecord;
      const content = outcome.response ?? outcome.detail;
      return content
        ? [{
          type: 'AssistantResponse',
          payload: {
            content,
            stop_reason: outcome.stop_reason,
            occurred_at_ms: Date.parse(outcome.finished_at),
          },
        }]
        : [];
    }

    if (record.type === 'ToolCall') {
      const call = record.payload as ToolCallRecord;
      return [{
        type: 'ToolCall',
        payload: {
          call_id: call.call_id,
          skill_name: call.skill_name,
          formatted_call: formatToolCall(call),
          occurred_at_ms: Date.parse(call.called_at),
        },
      }];
    }

    if (record.type === 'ToolResult') {
      const result = record.payload as ToolResultRecord;
      const ok = isResultOk(result.output);
      return [{
        type: 'ToolResult',
        payload: {
          call_id: result.call_id,
          skill_name: result.skill_name,
          success: ok,
          preview: resultPreview(result),
          occurred_at_ms: Date.parse(result.finished_at),
        },
      }];
    }

    if (record.type === 'PendingApproval') {
      const approval = record.payload as PendingApprovalRecord;
      return [{
        type: 'ApprovalRequested',
        payload: {
          resume_token: approval.resume_token,
          pending_calls: approval.pending_calls,
          occurred_at_ms: Date.parse(approval.created_at),
        },
      }];
    }

    if (record.type === 'Deliberation') {
      const deliberation = record.payload as any;
      return [{
        type: 'Plan',
        payload: {
          summary: deliberation.summary,
          candidate_actions: deliberation.candidate_actions ?? [],
          confidence: deliberation.confidence ?? 0,
          outcome: deliberation.outcome,
          occurred_at_ms: Date.parse(deliberation.created_at),
        },
      }];
    }

    if (record.type === 'ToolNodeCheckpoint') {
      const checkpoint = record.payload as ToolNodeCheckpointRecord;
      return [{
        type: 'ToolCheckpoint',
        payload: {
          call_id: checkpoint.call_id,
          skill_name: checkpoint.skill_name,
          status: checkpoint.status,
          attempt: checkpoint.attempt,
          detail: checkpoint.detail,
          occurred_at_ms: Date.parse(checkpoint.occurred_at),
        },
      }];
    }

    if (record.type === 'SkillInputValidation') {
      const validation = record.payload as SkillInputValidationRecord;
      return validation.valid ? [] : [{
        type: 'ValidationFailure',
        payload: {
          call_id: validation.call_id,
          skill_name: validation.skill_name,
          errors: validation.errors,
          occurred_at_ms: Date.parse(validation.validated_at),
        },
      }];
    }

    if (record.type === 'Reflection') {
      const reflection = record.payload as ReflectionRecord;
      return [{
        type: 'Learning',
        payload: {
          label: 'reflection',
          detail: reflection.summary,
          confidence: reflection.confidence,
          occurred_at_ms: Date.parse(reflection.created_at),
        },
      }];
    }

    if (record.type === 'PolicyTuning') {
      const tuning = record.payload as PolicyTuningRecord;
      return [{
        type: 'Learning',
        payload: {
          label: `policy ${tuning.action}`,
          detail: tuning.overlay.reason,
          confidence: tuning.overlay.confidence,
          occurred_at_ms: Date.parse(tuning.created_at),
        },
      }];
    }

    return [];
  });
}

function normalizeJsonValue(value: unknown): unknown {
  if (typeof value !== 'string') return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function formatCompactValue(value: unknown): string {
  const normalized = normalizeJsonValue(value);
  if (typeof normalized === 'string') return JSON.stringify(truncate(normalized, 80));
  if (typeof normalized === 'number' || typeof normalized === 'boolean' || normalized == null) {
    return String(normalized);
  }
  return truncate(JSON.stringify(normalized), 120);
}

function resultPreview(result: ToolResultRecord): string {
  if (isResultOk(result.output)) {
    const value = normalizeJsonValue((result.output as any).Ok);
    if (value && typeof value === 'object') {
      const object = value as Record<string, unknown>;
      if (typeof object.stdout === 'string') return truncate(object.stdout, 240);
      if (typeof object.content === 'string') return truncate(object.content, 240);
    }
    return formatCompactValue(value);
  }
  const error = (result.output as any).Err;
  return typeof error?.message === 'string' ? error.message : formatCompactValue(error);
}

function isResultOk(value: unknown): boolean {
  return !!value && typeof value === 'object' && 'Ok' in value;
}

function truncate(value: string, max = 160): string {
  return value.length > max ? `${value.slice(0, max - 1)}…` : value;
}
