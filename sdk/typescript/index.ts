/**
 * @rain-engine/sdk — RainEngine TypeScript SDK
 *
 * Re-exports all generated types and the client.
 */

// Generated domain types
export * from './rain-engine';

// Client SDK
export {
  RainEngineClient,
  RainEngineApiError,
  deriveSessionStatus,
  extractPendingApproval,
  formatToolCall,
  toTranscriptItems,
} from './client';
export type {
  ApprovalView,
  RainEngineClientOptions,
  RuntimeCapabilities,
  SelfImprovementView,
  SessionStatus,
  SessionStreamEvent,
  SessionStreamHandlers,
  SessionView,
  TimelineItem,
  ToolTimelineItem,
} from './client';
