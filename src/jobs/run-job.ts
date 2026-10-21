export interface RunJobParams {
  eventType: string;
  eventId: string;
  orderId: string;
  deliveryCount: number;
  timeoutMs: number;
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

/**
 * Validates a raw job message at the worker boundary. Returns null for
 * anything malformed so the consumer can discard it without requeue.
 */
export function parseRunJob(raw: unknown): RunJob | null {
  if (typeof raw !== 'object' || raw === null) {
    return null;
  }
  const candidate = raw as Record<string, unknown>;
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
  if (
    !isNonEmptyString(p.eventType) ||
    !isNonEmptyString(p.eventId) ||
    !isNonEmptyString(p.orderId) ||
    !isBoundedInt(p.deliveryCount, 1, 50) ||
    !isBoundedInt(p.timeoutMs, 1, 60000)
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
    },
  };
}
