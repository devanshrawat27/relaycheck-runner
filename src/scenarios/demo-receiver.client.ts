import { AttemptReport } from '../execution/executor';

/** Fixed paths on the demo receiver; the host comes only from configuration. */
const DELIVERIES_PATH = '/api/webhooks/demo';
const FAULTS_PATH = '/api/__test__/faults';
const ORACLE_PATH_PREFIX = '/api/__test__/orders/';
const DEFAULT_RECEIVER_URL = 'http://localhost:3100';
const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '::1']);

/**
 * Resolves the demo receiver base URL from configuration and refuses anything
 * that is not local. No request/job input ever influences this.
 */
export function resolveDemoReceiverBaseUrl(): URL {
  const raw = process.env.DEMO_RECEIVER_URL ?? DEFAULT_RECEIVER_URL;
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new Error('DEMO_RECEIVER_URL is not a valid URL');
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`DEMO_RECEIVER_URL must use http/https, got ${url.protocol}`);
  }
  if (!LOCAL_HOSTS.has(url.hostname)) {
    throw new Error(
      `Demo receiver target must be local (localhost/127.0.0.1), refusing ${url.hostname}`,
    );
  }
  url.pathname = '/';
  url.search = '';
  url.hash = '';
  return url;
}

export function describeError(error: unknown): string {
  const name =
    typeof error === 'object' && error !== null && 'name' in error
      ? String((error as { name: unknown }).name)
      : '';
  if (name === 'TimeoutError' || name === 'AbortError') {
    return 'timeout';
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'network error';
}

export interface DeliveryResult {
  report: AttemptReport;
  delivered: boolean;
}

/** Delivers one demo event and returns the recorded attempt payload. */
export async function deliverDemoEvent(
  receiver: URL,
  event: {
    eventId: string;
    eventType: string;
    orderId: string;
    amount: number;
    currency: string;
    occurredAt: string;
  },
  attemptNumber: number,
  timeoutMs: number,
): Promise<DeliveryResult> {
  const url = new URL(DELIVERIES_PATH, receiver).toString();
  const startedAt = Date.now();
  let httpStatus: number | undefined;
  let outcome: 'delivered' | 'error' | 'timeout' = 'delivered';
  let errorDetail: string | undefined;

  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(event),
      redirect: 'error',
      signal: AbortSignal.timeout(timeoutMs),
    });
    httpStatus = response.status;
    if (!response.ok) {
      outcome = 'error';
      errorDetail = `Receiver responded with HTTP ${response.status}`;
    }
  } catch (error) {
    const detail = describeError(error);
    outcome = detail === 'timeout' ? 'timeout' : 'error';
    errorDetail = outcome === 'timeout' ? `No response within ${timeoutMs} ms` : detail;
  }

  return {
    delivered: outcome === 'delivered',
    report: {
      attemptNumber,
      eventId: event.eventId,
      httpStatus,
      outcome,
      durationMs: Date.now() - startedAt,
      errorDetail: errorDetail?.slice(0, 200),
    },
  };
}

/**
 * Reads the receiver's minimum test-oracle state. A 404 means the order never
 * existed (0 effects) and is an assertion fact, not an execution error.
 */
export async function fetchBusinessEffectCount(
  receiver: URL,
  orderId: string,
  timeoutMs: number,
): Promise<number> {
  const url = new URL(`${ORACLE_PATH_PREFIX}${encodeURIComponent(orderId)}`, receiver).toString();

  let response: Response;
  try {
    response = await fetch(url, {
      redirect: 'error',
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch (error) {
    throw new Error(`Receiver test oracle unavailable: ${describeError(error)}`);
  }
  if (response.status === 404) {
    return 0;
  }
  if (!response.ok) {
    throw new Error(`Receiver test oracle returned HTTP ${response.status}`);
  }
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    throw new Error('Receiver test oracle returned a non-JSON payload');
  }
  const count = (body as { businessEffectCount?: unknown }).businessEffectCount;
  if (typeof count !== 'number') {
    throw new Error('Receiver test oracle payload is missing businessEffectCount');
  }
  return count;
}

export interface ReceiverFault {
  mode: 'error' | 'delay';
  httpStatus?: number;
  delayMs?: number;
}

/** Sets a controlled fault on the test-mode receiver; 404 means test mode is off. */
export async function setReceiverFault(receiver: URL, fault: ReceiverFault): Promise<void> {
  const url = new URL(FAULTS_PATH, receiver).toString();
  let response: Response;
  try {
    response = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(fault),
      signal: AbortSignal.timeout(3000),
    });
  } catch (error) {
    throw new Error(`Could not set receiver fault: ${describeError(error)}`);
  }
  if (response.status === 404) {
    throw new Error('Receiver fault controls are unavailable (RELAYCHECK_TEST_MODE is not enabled)');
  }
  if (!response.ok) {
    throw new Error(`Receiver fault control returned HTTP ${response.status}`);
  }
}

/** Clears receiver faults; best-effort so cleanup never masks the real result. */
export async function clearReceiverFault(receiver: URL): Promise<void> {
  const url = new URL(FAULTS_PATH, receiver).toString();
  try {
    await fetch(url, { method: 'DELETE', signal: AbortSignal.timeout(3000) });
  } catch {
    // Best-effort cleanup only.
  }
}

