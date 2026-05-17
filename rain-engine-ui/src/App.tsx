import { For, Show, createEffect, createMemo, createResource, createSignal, onCleanup } from 'solid-js';
import { css, cx } from 'styled-system/css';
import { flex } from 'styled-system/patterns';
import { SolidMarkdown } from 'solid-markdown';
import {
  ApprovalDecision,
  RainEngineClient,
  formatToolCall,
} from '@rain-engine/sdk';
import type {
  ApprovalView,
  RuntimeCapabilities,
  SessionSummary,
  SessionView,
  TimelineItem,
} from '@rain-engine/sdk';
import { Button } from './components/ui/button';
import { Input } from './components/ui/input';

const client = new RainEngineClient('http://127.0.0.1:8080');
const ACTOR_ID = 'developer';

export default function App() {
  const [currentSessionId, setCurrentSessionId] = createSignal<string | null>(null);
  const [query, setQuery] = createSignal('');

  const [capabilities] = createResource(loadCapabilities);
  const [sessions, { mutate: mutateSessions, refetch: refetchSessions }] = createResource(loadSessions);

  const filteredSessions = createMemo(() => {
    const needle = query().trim().toLowerCase();
    const list = sessions() ?? [];
    if (!needle) return list;
    return list.filter((session) => session.session_id.toLowerCase().includes(needle));
  });

  const handleNewSession = () => {
    const sessionId = `session-${crypto.randomUUID?.() ?? Date.now()}`;
    setCurrentSessionId(sessionId);
    mutateSessions((existing = []) => [
      {
        session_id: sessionId,
        first_recorded_at_ms: Date.now(),
        last_recorded_at_ms: Date.now(),
        record_count: 0,
      },
      ...existing,
    ]);
  };

  return (
    <div class={shellClass}>
      <SessionSidebar
        sessions={filteredSessions()}
        activeSessionId={currentSessionId()}
        loading={sessions.loading}
        query={query()}
        capabilities={capabilities()}
        onQuery={setQuery}
        onNewSession={handleNewSession}
        onSelectSession={setCurrentSessionId}
      />

      <main class={mainClass}>
        <Show
          when={currentSessionId()}
          fallback={<EmptyState capabilities={capabilities()} onNewSession={handleNewSession} />}
        >
          {(sessionId) => (
            <SessionWorkbench
              sessionId={sessionId()}
              capabilities={capabilities()}
              onActivity={refetchSessions}
            />
          )}
        </Show>
      </main>
    </div>
  );
}

function SessionSidebar(props: {
  sessions: SessionSummary[];
  activeSessionId: string | null;
  loading: boolean;
  query: string;
  capabilities?: RuntimeCapabilities;
  onQuery: (value: string) => void;
  onNewSession: () => void;
  onSelectSession: (sessionId: string) => void;
}) {
  return (
    <aside class={sidebarClass}>
      <div class={css({ p: '32px', borderBottom: '1px solid', borderColor: 'border.default' })}>
        <div class={css({ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: '24px' })}>
          <div>
            <div class={css({ fontSize: '18px', fontWeight: '900', letterSpacing: '-0.04em', color: 'fg.default' })}>RainEngine</div>
            <div class={css({ fontSize: '10px', color: 'fg.muted', mt: '2px', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: '700' })}>Control Room</div>
          </div>
          <StatusDot label={props.capabilities ? 'Online' : 'Offline'} active={!!props.capabilities} />
        </div>

        <Button 
          variant="solid" 
          width="full" 
          onClick={props.onNewSession}
          class={css({ bg: 'accent.default!', color: 'white!', fontWeight: '800', borderRadius: 'xl!', h: '44px!' })}
        >
          New Session
        </Button>
        <Input
          value={props.query}
          onInput={(event: any) => props.onQuery(event.currentTarget.value)}
          placeholder="Search sessions..."
          class={css({ w: 'full', mt: '16px', bg: 'bg.subtle!', border: '1px solid', borderColor: 'border.default!', fontSize: 'sm', h: '40px' })}
        />
      </div>

      <div class={css({ px: '24px', py: '16px', borderBottom: '1px solid', borderColor: 'border.default', bg: 'rgba(255,255,255,0.02)' })}>
        <div class={css({ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' })}>
          <CapabilityMetric label="Provider" value={props.capabilities?.provider_kind ?? 'offline'} />
          <CapabilityMetric label="Tools" value={String(props.capabilities?.skills.length ?? 0)} />
        </div>
      </div>

      <div class={css({ flex: 1, overflowY: 'auto', p: '12px', display: 'flex', flexDir: 'column', gap: '4px', minH: 0 })}>
        <Show when={props.loading}>
          <PanelNote>Syncing ledger…</PanelNote>
        </Show>
        <For each={props.sessions}>
          {(session) => (
            <button
              class={cx(sessionButtonClass, props.activeSessionId === session.session_id ? activeSessionButtonClass : '')}
              onClick={() => props.onSelectSession(session.session_id)}
            >
              <div class={css({ fontWeight: '700', fontSize: '13px' })}>
                {formatSessionLabel(session.session_id)}
              </div>
              <div class={css({ display: 'flex', justifyContent: 'space-between', mt: '4px', color: 'fg.muted', fontSize: '10px', opacity: 0.7 })}>
                <span>{session.record_count} items</span>
                <span>{formatRelativeTime(session.last_recorded_at_ms)}</span>
              </div>
            </button>
          )}
        </For>
      </div>
    </aside>
  );
}

function SessionWorkbench(props: {
  sessionId: string;
  capabilities?: RuntimeCapabilities;
  onActivity: () => void;
}) {
  const [view, setView] = createSignal<SessionView | null>(null);
  const [draft, setDraft] = createSignal('');
  const [error, setError] = createSignal<string | null>(null);
  const [busy, setBusy] = createSignal(false);
  const [approvalReason, setApprovalReason] = createSignal('');
  const [activeTab, setActiveTab] = createSignal<'timeline' | 'state' | 'tools' | 'graph' | 'learning' | 'raw'>('timeline');

  createEffect(() => {
    const sessionId = props.sessionId;
    setView(null);
    setError(null);
    void refreshView(sessionId, setView, setError);

    const source = client.streamSessionEvents(sessionId, {
      onView: (nextView) => {
        setView(nextView);
        props.onActivity();
        queueMicrotask(scrollTranscriptToBottom);
      },
      onError: () => setError('Link severed. Attempting to re-establish connection...'),
    });

    onCleanup(() => source.close());
  });

  const pendingApproval = createMemo(() => view()?.pending_approval ?? null);

  const sendInput = async () => {
    const content = draft().trim();
    if (!content || busy()) return;
    setBusy(true);
    setError(null);
    setDraft('');
    try {
      await client.sendHumanInput(ACTOR_ID, {
        session_id: props.sessionId,
        content,
      });
      props.onActivity();
    } catch (reason) {
      setError(errorMessage(reason, 'Transmission failure. Check gateway status.'));
      setDraft(content);
    } finally {
      setBusy(false);
    }
  };

  const submitApproval = async (decision: ApprovalDecision) => {
    const approval = pendingApproval();
    if (!approval || busy()) return;
    setBusy(true);
    setError(null);
    try {
      await client.submitApproval({
        session_id: props.sessionId,
        resume_token: approval.resume_token,
        decision,
        metadata: {
          actor_id: ACTOR_ID,
          client: 'rain-engine-ui',
          reason: approvalReason().trim(),
          decided_at_ms: Date.now(),
        },
      });
      setApprovalReason('');
      props.onActivity();
    } catch (reason) {
      setError(errorMessage(reason, 'Decision commit failed.'));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div class={workbenchClass}>
      <section class={transcriptPaneClass}>
        <SessionHeader sessionId={props.sessionId} view={view()} capabilities={props.capabilities} />

        <div id="transcript-scroll" class={transcriptScrollClass}>
          <Show when={view()} fallback={<PanelNote>Waiting for session synchronization…</PanelNote>}>
            {(sessionView) => (
              <div class={css({ maxW: '760px', w: 'full', mx: 'auto', display: 'flex', flexDir: 'column', gap: '32px', pb: '64px' })}>
                <For each={sessionView().timeline}>
                  {(item) => <TimelineRow item={item} />}
                </For>
                <Show when={busy()}>
                  <ThinkingRow />
                </Show>
              </div>
            )}
          </Show>
        </div>

        <Show when={pendingApproval()}>
          {(approval) => (
            <ApprovalPanel
              approval={approval()}
              reason={approvalReason()}
              busy={busy()}
              onReason={setApprovalReason}
              onApprove={() => submitApproval(ApprovalDecision.Approved)}
              onDeny={() => submitApproval(ApprovalDecision.Rejected)}
            />
          )}
        </Show>

        <Show when={error()}>
          {(message) => (
            <div class={errorClass}>
              <span>{message()}</span>
              <button class={linkButtonClass} onClick={() => void refreshView(props.sessionId, setView, setError)}>Reconnect</button>
            </div>
          )}
        </Show>

        <Composer
          value={draft()}
          disabled={busy() || !!pendingApproval()}
          onInput={setDraft}
          onSubmit={sendInput}
        />
      </section>

      <Inspector
        view={view()}
        capabilities={props.capabilities}
        activeTab={activeTab()}
        onTab={setActiveTab}
      />
    </div>
  );
}

function SessionHeader(props: {
  sessionId: string;
  view: SessionView | null;
  capabilities?: RuntimeCapabilities;
}) {
  return (
    <header class={sessionHeaderClass}>
      <div class={css({ display: 'flex', alignItems: 'center', gap: '16px' })}>
        <div class={css({ w: '40px', h: '40px', borderRadius: '12px', bg: 'accent.default', display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: '900', fontSize: '18px', color: 'white', boxShadow: '0 0 20px {colors.indigo.500/30}' })}>
          R
        </div>
        <div>
          <div class={css({ display: 'flex', alignItems: 'center', gap: '12px' })}>
            <h1 class={css({ fontSize: '15px', fontWeight: '800', letterSpacing: '-0.02em' })}>Session Workbench</h1>
            <StatusBadge status={props.view?.status ?? 'empty'} />
          </div>
          <p class={css({ color: 'fg.muted', fontSize: '10px', mt: '2px', fontFamily: 'var(--font-mono)', opacity: 0.6 })}>{props.sessionId}</p>
        </div>
      </div>
      <div class={css({ display: 'flex', gap: '24px', alignItems: 'center', fontSize: '12px', color: 'fg.muted' })}>
        <div class={css({ textAlign: 'right' })}>
          <div class={css({ fontWeight: '800', color: 'fg.default' })}>{props.view?.record_count ?? 0} records</div>
          <div class={css({ fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.05em' })}>{props.capabilities?.provider_kind ?? 'unknown'}</div>
        </div>
      </div>
    </header>
  );
}

function TimelineRow(props: { item: TimelineItem }) {
  const payload = () => props.item.payload as any;

  if (props.item.type === 'HumanInput') {
    return (
      <div class={flex({ justify: 'flex-end', animation: 'fadeIn 0.4s cubic-bezier(0.16, 1, 0.3, 1)' })}>
        <article class={humanBubbleClass}>
          <div class={bubbleMetaClass}>{payload().actor_id}</div>
          <div class={css({ fontSize: '15px', lineHeight: '1.6', fontWeight: '500' })}>{payload().content}</div>
        </article>
      </div>
    );
  }

  if (props.item.type === 'AssistantResponse') {
    return (
      <div class={flex({ justify: 'flex-start', animation: 'fadeIn 0.4s cubic-bezier(0.16, 1, 0.3, 1)' })}>
        <article class={assistantBubbleClass}>
          <div class={bubbleMetaClass}>Agent · {payload().stop_reason}</div>
          <div class={css({ fontSize: '15px', lineHeight: '1.6' })}>
            <SolidMarkdown children={payload().content} />
          </div>
        </article>
      </div>
    );
  }

  if (props.item.type === 'ToolCall') {
    return <CenterEvent tone="indigo" title="Skill Invocation" detail={payload().formatted_call} />;
  }

  if (props.item.type === 'ToolResult') {
    return <CenterEvent tone={payload().success ? 'success' : 'danger'} title={`${payload().skill_name} ${payload().success ? 'complete' : 'error'}`} detail={payload().preview} />;
  }

  if (props.item.type === 'ApprovalRequested') {
    return <CenterEvent tone="warning" title="Authorization Checkpoint" detail={`${payload().pending_calls.length} pending operations`} />;
  }

  if (props.item.type === 'ApprovalResolved') {
    return <CenterEvent tone="neutral" title="Checkpoint Resolved" detail={payload().decision} />;
  }

  if (props.item.type === 'Plan') {
    return <CenterEvent tone="indigo" title={`Reasoning Plan · ${(payload().confidence * 100).toFixed(0)}%`} detail={payload().summary} />;
  }

  if (props.item.type === 'ToolCheckpoint') {
    return <CenterEvent tone={payload().status === 'Succeeded' ? 'success' : payload().status === 'Failed' || payload().status === 'TimedOut' ? 'danger' : 'neutral'} title={`Checkpoint · ${payload().status}`} detail={`${payload().skill_name} · attempt ${payload().attempt}`} />;
  }

  if (props.item.type === 'ValidationFailure') {
    return <CenterEvent tone="danger" title="Schema Rejected" detail={`${payload().skill_name}: ${payload().errors.join(', ')}`} />;
  }

  if (props.item.type === 'Learning') {
    return <CenterEvent tone="indigo" title={`Learning · ${(payload().confidence * 100).toFixed(0)}%`} detail={payload().detail} />;
  }

  return <CenterEvent tone="neutral" title={payload().label || 'Kernel Notification'} detail={payload().detail || ''} />;
}

function CenterEvent(props: { tone: 'neutral' | 'success' | 'danger' | 'warning' | 'indigo'; title: string; detail: string }) {
  return (
    <div class={flex({ justify: 'center' })}>
      <div class={cx(centerEventClass, toneClass(props.tone))}>
        <span class={css({ fontWeight: '900', textTransform: 'uppercase', letterSpacing: '0.15em', fontSize: '9px', opacity: 0.6 })}>{props.title}</span>
        <span class={css({ fontWeight: '600', fontSize: '12px', textAlign: 'center', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxW: '480px' })}>{props.detail}</span>
      </div>
    </div>
  );
}

function ApprovalPanel(props: {
  approval: ApprovalView;
  reason: string;
  busy: boolean;
  onReason: (value: string) => void;
  onApprove: () => void;
  onDeny: () => void;
}) {
  return (
    <section class={approvalPanelClass}>
      <div class={css({ display: 'grid', gridTemplateColumns: '1fr auto', gap: '24px', alignItems: 'start' })}>
        <div>
          <div class={css({ fontSize: '10px', color: 'amber.600', fontWeight: '900', textTransform: 'uppercase', letterSpacing: '0.2em' })}>Attention Required</div>
          <h2 class={css({ fontSize: '20px', fontWeight: '900', mt: '4px', letterSpacing: '-0.03em' })}>Authorize Capabilities</h2>
          <p class={css({ fontSize: '13px', color: 'slate.700', mt: '4px', fontWeight: '500' })}>{props.approval.reason}</p>
        </div>
        <div class={css({ fontSize: '11px', color: 'slate.500', fontFamily: 'var(--font-mono)', bg: 'white', px: '12px', py: '4px', borderRadius: '8px', border: '1px solid {colors.amber.200}' })}>STEP {props.approval.step}</div>
      </div>

      <div class={css({ display: 'flex', flexDir: 'column', gap: '12px', my: '24px', maxH: '400px', overflowY: 'auto', pr: '8px', minH: 0 })}>
        <For each={props.approval.pending_calls}>
          {(call) => (
            <div class={approvalCallClass}>
              <div class={css({ fontFamily: 'var(--font-mono)', fontSize: '12px', fontWeight: '800', mb: '8px', color: 'slate.900' })}>
                {formatToolCall(call)}
              </div>
              <pre class={jsonBlockClass}>{prettyJson(call.args)}</pre>
            </div>
          )}
        </For>
      </div>

      <textarea
        value={props.reason}
        onInput={(event) => props.onReason(event.currentTarget.value)}
        placeholder="Rationale for decision (optional)..."
        class={approvalReasonClass}
      />

      <div class={css({ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: '20px' })}>
        <div class={css({ fontSize: '10px', color: 'slate.500', opacity: 0.6, fontFamily: 'var(--font-mono)' })}>
          TOKEN: {props.approval.resume_token.slice(0, 12)}
        </div>
        <div class={css({ display: 'flex', gap: '12px' })}>
          <Button variant="ghost" size="sm" disabled={props.busy} onClick={props.onDeny} class={css({ color: 'red.600!', fontWeight: '800' })}>Reject</Button>
          <Button size="lg" disabled={props.busy} onClick={props.onApprove} class={css({ bg: 'green.600!', color: 'white!', fontWeight: '900', px: '32px!' })}>Authorize</Button>
        </div>
      </div>
    </section>
  );
}

function Composer(props: {
  value: string;
  disabled: boolean;
  onInput: (value: string) => void;
  onSubmit: () => void;
}) {
  return (
    <div class={composerWrapperClass}>
      <form
        class={composerClass}
        onSubmit={(event) => {
          event.preventDefault();
          props.onSubmit();
        }}
      >
        <textarea
          value={props.value}
          disabled={props.disabled}
          onInput={(event) => props.onInput(event.currentTarget.value)}
          placeholder={props.disabled ? 'Authorizing kernel operations...' : 'Dispatch command...'}
          class={composerInputClass}
          onKeyDown={(event) => {
            if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') props.onSubmit();
          }}
        />
        <div class={css({ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: '12px' })}>
          <div class={css({ fontSize: '10px', color: 'fg.muted', opacity: 0.5, fontWeight: '600' })}>
            ⌘ + ENTER TO DISPATCH
          </div>
          <Button 
            size="sm"
            type="submit" 
            disabled={props.disabled || !props.value.trim()}
            class={css({ bg: 'accent.default!', color: 'white!', fontWeight: '800', px: '24px!' })}
          >
            Dispatch
          </Button>
        </div>
      </form>
    </div>
  );
}

function Inspector(props: {
  view: SessionView | null;
  capabilities?: RuntimeCapabilities;
  activeTab: 'timeline' | 'state' | 'tools' | 'graph' | 'learning' | 'raw';
  onTab: (tab: 'timeline' | 'state' | 'tools' | 'graph' | 'learning' | 'raw') => void;
}) {
  const tabs = ['timeline', 'state', 'tools', 'graph', 'learning', 'raw'] as const;
  return (
    <aside class={inspectorClass}>
      <div class={css({ p: '24px', borderBottom: '1px solid', borderColor: 'border.default' })}>
        <div class={css({ fontSize: '13px', fontWeight: '900', letterSpacing: '0.1em', textTransform: 'uppercase', color: 'fg.default' })}>Inspector</div>
      </div>

      <div class={css({ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', px: '4px', pt: '4px' })}>
        <For each={tabs}>
          {(tab) => (
            <button class={cx(tabButtonClass, props.activeTab === tab ? activeTabButtonClass : '')} onClick={() => props.onTab(tab)}>
              {tab}
            </button>
          )}
        </For>
      </div>

      <div class={css({ flex: 1, overflowY: 'auto', p: '24px', minH: 0 })}>
        <Show when={props.view} fallback={<PanelNote>Select a session to begin analysis.</PanelNote>}>
          {(view) => (
            <div class={css({ animation: 'slideInRight 0.3s ease-out' })}>
              <Show when={props.activeTab === 'timeline'}>
                <RecordStats view={view()} capabilities={props.capabilities} />
              </Show>
              <Show when={props.activeTab === 'state'}>
                <StatePanel view={view()} />
              </Show>
              <Show when={props.activeTab === 'tools'}>
                <ToolsPanel view={view()} capabilities={props.capabilities} />
              </Show>
              <Show when={props.activeTab === 'graph'}>
                <ExecutionGraphPanel view={view()} />
              </Show>
              <Show when={props.activeTab === 'learning'}>
                <LearningPanel view={view()} />
              </Show>
              <Show when={props.activeTab === 'raw'}>
                <pre class={jsonBlockClass}>{prettyJson(view())}</pre>
              </Show>
            </div>
          )}
        </Show>
      </div>
    </aside>
  );
}

function RecordStats(props: { view: SessionView; capabilities?: RuntimeCapabilities }) {
  return (
    <div class={css({ display: 'flex', flexDir: 'column', gap: '16px' })}>
      <MetricCard label="Current Status" value={props.view.status} />
      <div class={css({ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' })}>
        <MetricCard label="Records" value={String(props.view.record_count)} />
        <MetricCard label="Cost (Est)" value={`$${props.view.total_estimated_cost_usd.toFixed(4)}`} />
      </div>
      <MetricCard label="Active Provider" value={props.capabilities?.provider_kind ?? 'Unknown'} />
      <MetricCard label="Skill Sets" value={String(props.capabilities?.skills.length ?? 0)} />
    </div>
  );
}

function StatePanel(props: { view: SessionView }) {
  const state = () => props.view.state;
  return (
    <div class={css({ display: 'flex', flexDir: 'column', gap: '16px' })}>
      <div class={css({ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' })}>
        <MetricCard label="Active Goals" value={String(state().goals.length)} />
        <MetricCard label="Task Backlog" value={String(state().tasks.length)} />
      </div>
      <Show when={state().tasks.length > 0}>
        <div class={panelCardClass}>
          <div class={panelLabelClass}>Priority Queue</div>
          <div class={css({ mt: '12px', display: 'flex', flexDir: 'column', gap: '8px' })}>
            <For each={state().tasks}>
              {(task) => (
                <div class={css({ py: '8px', borderTop: '1px solid', borderColor: 'border.default', fontSize: '11px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' })}>
                  <span class={css({ fontWeight: '700', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' })}>{task.title}</span>
                  <span class={css({ color: 'accent.default', fontWeight: '900', fontSize: '9px', textTransform: 'uppercase', ml: '12px' })}>{task.status}</span>
                </div>
              )}
            </For>
          </div>
        </div>
      </Show>
    </div>
  );
}

function ToolsPanel(props: { view: SessionView; capabilities?: RuntimeCapabilities }) {
  return (
    <div class={css({ display: 'flex', flexDir: 'column', gap: '16px' })}>
      <Show when={props.view.tool_timeline.length > 0} fallback={<PanelNote>No invocations recorded.</PanelNote>}>
        <For each={props.view.tool_timeline}>
          {(tool) => (
            <div class={panelCardClass}>
              <div class={css({ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: '8px' })}>
                <span class={css({ fontWeight: '900', fontSize: '11px', color: 'fg.default' })}>{tool.skill_name}</span>
                <span class={cx(css({ fontSize: '9px', fontWeight: '900', px: '6px', py: '2px', borderRadius: '4px', textTransform: 'uppercase' }), tool.success ? css({ bg: 'green.500/20', color: 'green.400' }) : css({ bg: 'red.500/20', color: 'red.400' }))}>
                  {tool.success ? 'Success' : 'Error'}
                </span>
              </div>
              <Show when={tool.output_preview}>
                <pre class={css({ mt: '12px', p: '12px', bg: 'slate.950', borderRadius: '8px', fontSize: '10px', color: 'green.400', overflowX: 'auto', border: '1px solid {colors.white/5}' })}>
                  {tool.output_preview}
                </pre>
              </Show>
            </div>
          )}
        </For>
      </Show>
    </div>
  );
}

function ExecutionGraphPanel(props: { view: SessionView }) {
  const graph = () => props.view.execution_graph;
  const latestStatus = createMemo(() => {
    const map = new Map<string, string>();
    for (const checkpoint of graph().checkpoints) {
      map.set(checkpoint.call_id, checkpoint.status);
    }
    return map;
  });
  const validationFailures = createMemo(() => graph().validations.filter((validation) => !validation.valid));

  return (
    <div class={css({ display: 'flex', flexDir: 'column', gap: '16px' })}>
      <div class={css({ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' })}>
        <MetricCard label="Graphs" value={String(graph().graphs.length)} />
        <MetricCard label="Checkpoints" value={String(graph().checkpoints.length)} />
      </div>

      <Show when={graph().active_graph} fallback={<PanelNote>No active execution graph.</PanelNote>}>
        {(active) => (
          <div class={panelCardClass}>
            <div class={panelLabelClass}>Active Graph</div>
            <div class={css({ mt: '8px', fontSize: '11px', fontFamily: 'var(--font-mono)', color: 'fg.muted' })}>{active().graph_id}</div>
            <div class={css({ mt: '12px', display: 'flex', flexDir: 'column', gap: '8px' })}>
              <For each={active().nodes}>
                {(node) => {
                  const status = () => latestStatus().get(node.call_id) ?? 'Queued';
                  return (
                    <div class={css({ p: '12px', borderRadius: '12px', bg: 'white/4', border: '1px solid', borderColor: graph().blocked_call_ids.includes(node.call_id) ? 'red.500/40' : 'white/8' })}>
                      <div class={css({ display: 'flex', justifyContent: 'space-between', gap: '12px', alignItems: 'center' })}>
                        <span class={css({ fontWeight: '900', fontSize: '12px' })}>{node.skill_name}</span>
                        <span class={css({ fontSize: '9px', fontWeight: '900', textTransform: 'uppercase', color: status() === 'Succeeded' ? 'green.400' : status() === 'Failed' || status() === 'TimedOut' ? 'red.400' : 'fg.muted' })}>{status()}</span>
                      </div>
                      <div class={css({ mt: '6px', fontSize: '10px', color: 'fg.muted' })}>
                        priority {node.priority} · depends on {node.dependencies.length || 'none'} · attempts {node.retry_policy.policy.max_attempts}
                      </div>
                    </div>
                  );
                }}
              </For>
            </div>
          </div>
        )}
      </Show>

      <Show when={validationFailures().length > 0}>
        <div class={panelCardClass}>
          <div class={panelLabelClass}>Validation Failures</div>
          <For each={validationFailures()}>
            {(failure) => (
              <div class={learningItemClass}>
                <div class={css({ fontWeight: '900', fontSize: '12px', color: 'red.400' })}>{failure.skill_name}</div>
                <div class={css({ mt: '4px', fontSize: '11px', color: 'fg.muted', lineHeight: '1.5' })}>{failure.errors.join(', ')}</div>
              </div>
            )}
          </For>
        </div>
      </Show>
    </div>
  );
}

function LearningPanel(props: { view: SessionView }) {
  const learning = () => props.view.self_improvement;
  return (
    <div class={css({ display: 'flex', flexDir: 'column', gap: '16px' })}>
      <MetricCard
        label="Active Overlay"
        value={learning().active_overlay ? learning().active_overlay?.status ?? 'Applied' : 'None'}
      />
      <Show when={learning().active_overlay}>
        {(overlay) => (
          <div class={panelCardClass}>
            <div class={panelLabelClass}>Current Tuning</div>
            <div class={css({ mt: '8px', fontSize: '13px', fontWeight: '700', lineHeight: '1.5' })}>{overlay().reason}</div>
            <pre class={jsonBlockClass}>{prettyJson(overlay().patch)}</pre>
          </div>
        )}
      </Show>

      <div class={panelCardClass}>
        <div class={panelLabelClass}>Reflections</div>
        <Show when={learning().reflections.length > 0} fallback={<div class={css({ mt: '12px', fontSize: '12px', color: 'fg.muted' })}>No reflections yet.</div>}>
          <For each={learning().reflections.slice().reverse().slice(0, 6)}>
            {(reflection) => (
              <div class={learningItemClass}>
                <div class={css({ fontWeight: '800', fontSize: '12px' })}>{reflection.summary}</div>
                <div class={css({ mt: '4px', fontSize: '10px', color: 'fg.muted' })}>confidence {(reflection.confidence * 100).toFixed(0)}%</div>
              </div>
            )}
          </For>
        </Show>
      </div>

      <div class={panelCardClass}>
        <div class={panelLabelClass}>Policy Changes</div>
        <Show when={learning().policy_tunings.length > 0} fallback={<div class={css({ mt: '12px', fontSize: '12px', color: 'fg.muted' })}>No policy changes applied.</div>}>
          <For each={learning().policy_tunings.slice().reverse().slice(0, 6)}>
            {(tuning) => (
              <div class={learningItemClass}>
                <div class={css({ display: 'flex', justifyContent: 'space-between', gap: '12px' })}>
                  <span class={css({ fontWeight: '900', fontSize: '11px', textTransform: 'uppercase' })}>{tuning.action}</span>
                  <span class={css({ fontSize: '10px', color: 'fg.muted' })}>{(tuning.overlay.confidence * 100).toFixed(0)}%</span>
                </div>
                <div class={css({ mt: '4px', fontSize: '12px', color: 'fg.muted', lineHeight: '1.5' })}>{tuning.overlay.reason}</div>
              </div>
            )}
          </For>
        </Show>
      </div>

      <div class={panelCardClass}>
        <div class={panelLabelClass}>Tool Learning</div>
        <Show when={learning().tool_performance.length > 0} fallback={<div class={css({ mt: '12px', fontSize: '12px', color: 'fg.muted' })}>No tool performance evidence yet.</div>}>
          <For each={learning().tool_performance.slice().reverse().slice(0, 8)}>
            {(tool) => (
              <div class={learningItemClass}>
                <div class={css({ display: 'flex', justifyContent: 'space-between' })}>
                  <span class={css({ fontWeight: '800', fontSize: '12px' })}>{tool.skill_name}</span>
                  <span class={css({ fontSize: '10px', color: tool.failure_rate > 0.5 ? 'red.400' : 'green.400', fontWeight: '900' })}>{(tool.failure_rate * 100).toFixed(0)}% fail</span>
                </div>
                <div class={css({ mt: '4px', fontSize: '10px', color: 'fg.muted' })}>{tool.successes} ok · {tool.failures} failed · {tool.backend_kind}</div>
              </div>
            )}
          </For>
        </Show>
      </div>
    </div>
  );
}

function EmptyState(props: { capabilities?: RuntimeCapabilities; onNewSession: () => void }) {
  return (
    <div class={emptyStateClass}>
      <div class={css({ display: 'flex', flexDir: 'column', gap: '12px' })}>
        <div class={css({ fontSize: '14px', color: 'accent.default', fontWeight: '900', textTransform: 'uppercase', letterSpacing: '0.3em' })}>
          {props.capabilities ? 'KERNEL_LINK_ESTABLISHED' : 'SYNCHRONIZING...'}
        </div>
        <h1 class={css({ fontSize: '72px', fontWeight: '900', letterSpacing: '-0.06em', lineHeight: '0.9', color: 'fg.default' })}>
          Durable <br/>Agent Flow.
        </h1>
      </div>
      <p class={css({ color: 'fg.muted', maxW: '480px', fontSize: '20px', lineHeight: '1.6', fontWeight: '500' })}>
        Orchestrate multi-step agentic workflows with granular human oversight and real-time state projections.
      </p>
      <Button 
        size="lg" 
        onClick={props.onNewSession}
        class={css({ bg: 'fg.default!', color: 'bg.canvas!', fontWeight: '900', borderRadius: '2xl!', px: '48px!', h: '64px!', fontSize: '18px!', _hover: { transform: 'scale(1.02)' } })}
      >
        Initialize Session
      </Button>
    </div>
  );
}

function ThinkingRow() {
  return <CenterEvent tone="indigo" title="Cognition Active" detail="Model is projecting next session state..." />;
}

function StatusBadge(props: { status: string }) {
  return <span class={cx(statusBadgeClass, statusClass(props.status))}>{props.status}</span>;
}

function StatusDot(props: { label: string; active: boolean }) {
  return (
    <div class={css({ display: 'flex', alignItems: 'center', gap: '10px' })}>
       <span class={css({
         w: '8px',
         h: '8px',
         borderRadius: 'full',
         bg: props.active ? 'green.500' : 'red.500',
         boxShadow: props.active ? '0 0 12px {colors.green.500}' : 'none',
       })} />
       <span class={css({ fontSize: '10px', fontWeight: '900', color: 'fg.muted', textTransform: 'uppercase', letterSpacing: '0.1em' })}>
         {props.active ? 'Sync' : 'Offline'}
       </span>
    </div>
  );
}

function CapabilityMetric(props: { label: string; value: string }) {
  return (
    <div class={css({ bg: 'bg.subtle', border: '1px solid', borderColor: 'border.default', borderRadius: '16px', p: '12px' })}>
      <div class={panelLabelClass}>{props.label}</div>
      <div class={css({ fontSize: '12px', fontWeight: '800', mt: '4px', color: 'fg.default' })}>{props.value}</div>
    </div>
  );
}

function MetricCard(props: { label: string; value: string }) {
  return (
    <div class={panelCardClass}>
      <div class={panelLabelClass}>{props.label}</div>
      <div class={css({ fontSize: '24px', fontWeight: '900', mt: '4px', color: 'fg.default', letterSpacing: '-0.02em' })}>{props.value}</div>
    </div>
  );
}

function PanelNote(props: { children: any }) {
  return <div class={css({ p: '32px', color: 'fg.muted', fontSize: '13px', textAlign: 'center', border: '1px dashed', borderColor: 'border.default', borderRadius: '24px', lineHeight: '1.6' })}>{props.children}</div>;
}

async function loadSessions(): Promise<SessionSummary[]> {
  try {
    return await client.listSessions();
  } catch {
    return [];
  }
}

async function loadCapabilities(): Promise<RuntimeCapabilities | undefined> {
  try {
    return await client.getCapabilities();
  } catch {
    return undefined;
  }
}

async function refreshView(
  sessionId: string,
  setView: (view: SessionView | null) => void,
  setError: (message: string | null) => void,
) {
  try {
    setView(await client.getSessionView(sessionId));
  } catch (reason: any) {
    if (reason?.status === 404 || reason?.status === 500) {
      setView(null);
      return;
    }
    setError(errorMessage(reason, 'Session synchronization failure.'));
  }
}

function scrollTranscriptToBottom() {
  const container = document.getElementById('transcript-scroll');
  if (container) container.scrollTop = container.scrollHeight;
}

function prettyJson(value: unknown): string {
  if (typeof value === 'string') {
    try {
      return JSON.stringify(JSON.parse(value), null, 2);
    } catch {
      return value;
    }
  }
  return JSON.stringify(value, null, 2);
}

function errorMessage(reason: unknown, fallback: string): string {
  if (reason instanceof Error) return reason.message;
  return fallback;
}

function formatSessionLabel(id: string): string {
  if (id.startsWith('session-')) return id.slice(8, 22);
  return id.length > 18 ? `${id.slice(0, 18)}…` : id;
}

function formatRelativeTime(ms: number): string {
  const delta = Date.now() - ms;
  if (delta < 60_000) return 'NOW';
  if (delta < 3_600_000) return `${Math.floor(delta / 60_000)}M`;
  if (delta < 86_400_000) return `${Math.floor(delta / 3_600_000)}H`;
  return `${Math.floor(delta / 86_400_000)}D`;
}

const shellClass = flex({
  h: '100vh',
  w: '100vw',
  bg: 'bg.canvas',
  color: 'fg.default',
  overflow: 'hidden',
});

const sidebarClass = flex({
  direction: 'column',
  w: '300px',
  minW: '300px',
  bg: 'bg.sidebar',
  backdropFilter: 'blur(12px)',
  borderRight: '1px solid',
  borderColor: 'border.default',
  h: '100vh',
});

const mainClass = css({ flex: 1, minW: 0, bg: 'bg.canvas', position: 'relative', h: '100vh', overflow: 'hidden' });
const workbenchClass = css({ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) 360px', h: '100vh', overflow: 'hidden' });
const transcriptPaneClass = flex({ direction: 'column', minW: 0, h: '100vh', bg: 'transparent', minH: 0, overflow: 'hidden' });
const transcriptScrollClass = css({ flex: 1, overflowY: 'auto', px: '32px', py: '48px', minH: 0, scrollBehavior: 'smooth' });
const inspectorClass = flex({ direction: 'column', bg: 'bg.sidebar', backdropFilter: 'blur(12px)', borderLeft: '1px solid', borderColor: 'border.default', minW: '360px', h: '100vh', overflow: 'hidden' });
const sessionHeaderClass = css({ px: '32px', py: '24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', borderBottom: '1px solid', borderColor: 'border.default', bg: 'hsla(240, 10%, 4%, 0.7)', backdropFilter: 'blur(12px)', position: 'sticky', top: 0, zIndex: 100 });
const sessionButtonClass = css({ w: 'full', px: '16px', py: '12px', borderRadius: '12px', textAlign: 'left', cursor: 'pointer', border: '1px solid transparent', bg: 'transparent', transition: 'all 0.2s cubic-bezier(0.4, 0, 0.2, 1)', _hover: { bg: 'bg.subtle', borderColor: 'border.default' } });
const activeSessionButtonClass = css({ bg: 'accent.default/15!', borderColor: 'accent.default!', boxShadow: '0 0 25px {colors.indigo.500/10}' });
const humanBubbleClass = css({ bg: 'accent.default', color: 'white', px: '24px!', py: '16px!', borderRadius: '24px', borderBottomRightRadius: '4px', maxW: '75%', boxShadow: '0 8px 30px {colors.indigo.500/30}' });
const assistantBubbleClass = css({ bg: 'bg.card', border: '1px solid', borderColor: 'border.default', px: '24px!', py: '16px!', borderRadius: '24px', borderBottomLeftRadius: '4px', maxW: '85%', boxShadow: '0 4px 20px rgba(0,0,0,0.3)' });
const bubbleMetaClass = css({ fontSize: '9px', color: 'fg.muted', mb: '8px', fontWeight: '900', textTransform: 'uppercase', letterSpacing: '0.1em', opacity: 0.8 });
const centerEventClass = css({ display: 'flex', flexDir: 'column', gap: '4px', alignItems: 'center', px: '24px', py: '12px', borderRadius: '16px', border: '1px solid', borderColor: 'border.default', bg: 'bg.subtle', color: 'fg.muted', mx: 'auto', w: 'fit-content', backdropFilter: 'blur(4px)' });
const approvalPanelClass = css({ mx: '32px', mb: '24px', p: '24px', bg: 'amber.50/95', color: 'slate.950', border: '2px solid', borderColor: 'amber.400', borderRadius: '24px', boxShadow: '0 20px 50px rgba(245, 158, 11, 0.2)', animation: 'fadeIn 0.5s ease-out' });
const approvalCallClass = css({ bg: 'white', border: '1px solid', borderColor: 'amber.200', borderRadius: '16px', p: '16px', boxShadow: 'sm' });
const approvalReasonClass = css({ w: 'full', minH: '80px', p: '16px', borderRadius: '16px', border: '1px solid', borderColor: 'amber.200', bg: 'white', resize: 'none', outline: 'none', fontSize: '13px', mt: '12px', _focus: { borderColor: 'amber.500' } });
const composerWrapperClass = css({ px: '32px', pb: '32px', pt: '8px' });
const composerClass = css({ p: '20px', borderRadius: '24px', border: '1px solid', borderColor: 'border.default', bg: 'bg.card', boxShadow: '0 15px 40px rgba(0,0,0,0.4)', backdropFilter: 'blur(8px)' });
const composerInputClass = css({ w: 'full', minH: '56px', maxH: '200px', p: '0', borderRadius: 'none', border: 'none', bg: 'transparent', color: 'fg.default', resize: 'none', outline: 'none', fontSize: '15px', lineHeight: '1.6' });
const errorClass = css({ mx: '32px', mb: '16px', p: '16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', borderRadius: '16px', bg: 'red.500/10', color: 'red.400', fontSize: '13px', border: '1px solid', borderColor: 'red.500/20' });
const linkButtonClass = css({ color: 'red.400', fontWeight: '900', textDecoration: 'underline', cursor: 'pointer', ml: '12px' });
const jsonBlockClass = css({ p: '16px', bg: 'slate.950', color: 'slate.50', borderRadius: '12px', overflowX: 'auto', whiteSpace: 'pre-wrap', fontSize: '11px', fontFamily: 'var(--font-mono)', border: '1px solid', borderColor: 'white/5' });
const tabButtonClass = css({ flex: 1, py: '16px', fontSize: '10px', fontWeight: '900', color: 'fg.muted', cursor: 'pointer', borderBottom: '2px solid transparent', textTransform: 'uppercase', letterSpacing: '0.1em', transition: 'all 0.2s' });
const activeTabButtonClass = css({ color: 'fg.default!', borderBottomColor: 'accent.default!' });
const panelCardClass = css({ p: '20px', border: '1px solid', borderColor: 'border.default', bg: 'bg.subtle', borderRadius: '20px', backdropFilter: 'blur(4px)' });
const panelLabelClass = css({ fontSize: '9px', color: 'fg.muted', textTransform: 'uppercase', letterSpacing: '0.15em', fontWeight: '900', opacity: 0.7 });
const learningItemClass = css({ mt: '12px', pt: '12px', borderTop: '1px solid', borderColor: 'border.default' });
const emptyStateClass = css({ h: 'full', display: 'flex', flexDir: 'column', alignItems: 'flex-start', justifyContent: 'center', gap: '32px', px: '64px', background: 'radial-gradient(circle at 0% 0%, {colors.indigo.500/10}, transparent 50%), radial-gradient(circle at 100% 100%, {colors.violet.500/5}, transparent 50%)' });
const statusBadgeClass = css({ px: '10px', py: '4px', borderRadius: '8px', fontSize: '10px', fontWeight: '900', textTransform: 'uppercase', letterSpacing: '0.05em' });

function toneClass(tone: 'neutral' | 'success' | 'danger' | 'warning' | 'indigo') {
  return ({
    neutral: css({ borderColor: 'border.default', color: 'fg.muted' }),
    success: css({ borderColor: 'green.500/30', color: 'green.400', bg: 'green.500/10' }),
    danger: css({ borderColor: 'red.500/30', color: 'red.400', bg: 'red.500/10' }),
    warning: css({ borderColor: 'amber.500/30', color: 'amber.400', bg: 'amber.500/10' }),
    indigo: css({ borderColor: 'indigo.500/30', color: 'indigo.400', bg: 'indigo.500/10' }),
  })[tone];
}

function statusClass(status: string) {
  if (status === 'suspended') return css({ bg: 'amber.500/20', color: 'amber.400' });
  if (status === 'failed') return css({ bg: 'red.500/20', color: 'red.400' });
  if (status === 'running') return css({ bg: 'indigo.500/20', color: 'indigo.400' });
  if (status === 'completed') return css({ bg: 'green.500/20', color: 'green.400' });
  return css({ bg: 'bg.subtle', color: 'fg.muted' });
}
