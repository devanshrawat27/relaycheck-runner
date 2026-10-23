export interface RunJobParams {
  eventType: string;
  eventId: string;
  orderId: string;
  deliveryCount: number;
  timeoutMs: number;
  httpStatus?: number;
  delayMs?: number;
  targetName: string;
  targetUrl: string;
  targetOracle: boolean;
}

/** Versioned job contract published by the Control API. */
export interface RunJob {
  schemaVersion: 1;
  runId: string;
  scenario: string;
  params: RunJobParams;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function isBoundedInt(value: unknown, min: number, max: number): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value >= min && value <= max;
}

const TOP_LEVEL_KEYS = new Set(['schemaVersion', 'runId', 'scenario', 'params']);
const PARAM_KEYS = new Set([
  'eventType',
  'eventId',
  'orderId',
  'deliveryCount',
  'timeoutMs',
  'httpStatus',
  'delayMs',
  'targetName',
  'targetUrl',
  'targetOracle',
]);

function hasOnlyKeys(value: Record<string, unknown>, allowed: Set<string>): boolean {
  return Object.keys(value).every((key) => allowed.has(key));
}

/**
 * Validates a raw job message at the worker boundary. Returns null for
 * anything malformed so the consumer can discard it without requeue.
 * Unknown keys are rejected outright — a job can never smuggle a
 * destination URL or any other extra field.
 */
export function parseRunJob(raw: unknown): RunJob | null {
  if (typeof raw !== 'object' || raw === null) {
    return null;
  }
  const candidate = raw as Record<string, unknown>;
  if (!hasOnlyKeys(candidate, TOP_LEVEL_KEYS)) {
    return null;
  }
  if (candidate.schemaVersion !== 1) {
    return null;
  }
  if (!isNonEmptyString(candidate.runId) || !isNonEmptyString(candidate.scenario)) {
    return null;
  }
  const params = candidate.params;
  if (typeof params !== 'object' || params === null) {
    return null;
  }
  const p = params as Record<string, unknown>;
  if (!hasOnlyKeys(p, PARAM_KEYS)) {
    return null;
  }
  if (
    !isNonEmptyString(p.eventType) ||
    !isNonEmptyString(p.eventId) ||
    !isNonEmptyString(p.orderId) ||
    !isBoundedInt(p.deliveryCount, 1, 50) ||
    !isBoundedInt(p.timeoutMs, 1, 60000)
  ) {
    return null;
  }
  if (p.httpStatus !== undefined && !isBoundedInt(p.httpStatus, 400, 599)) {
    return null;
  }
  if (p.delayMs !== undefined && !isBoundedInt(p.delayMs, 0, 10000)) {
    return null;
  }
  if (
    !isNonEmptyString(p.targetName) ||
    !isNonEmptyString(p.targetUrl) ||
    typeof p.targetOracle !== 'boolean'
  ) {
    return null;
  }
  return {
    schemaVersion: 1,
    runId: candidate.runId,
    scenario: candidate.scenario,
    params: {
      eventType: p.eventType,
      eventId: p.eventId,
      orderId: p.orderId,
      deliveryCount: p.deliveryCount,
      timeoutMs: p.timeoutMs,
      ...(typeof p.httpStatus === 'number' ? { httpStatus: p.httpStatus } : {}),
      ...(typeof p.delayMs === 'number' ? { delayMs: p.delayMs } : {}),
      targetName: p.targetName,
      targetUrl: p.targetUrl,
      targetOracle: p.targetOracle,
    },
  };
}


