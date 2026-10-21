import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { AssertionReport, ExecutionContext, ExecutionResult, ScenarioExecutor } from '../execution/executor';
import { ExecutorRegistry } from './../execution/executor-registry';

/** Fixed delivery path on the demo receiver; the host comes only from configuration. */
const DELIVERIES_PATH = '/api/webhooks/demo';
/** Fixed test-oracle path (minimum business state; local/test mode only). */
const ORACLE_PATH_PREFIX = '/api/__test__/orders/';
const DEFAULT_RECEIVER_URL = 'http://localhost:3100';
const AMOUNT_MINOR_UNITS = 5000;
const CURRENCY = 'USD';
const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '::1']);

export const ASSERTION_ATTEMPT_COUNT = 'delivery_attempt_count';
export const ASSERTION_DELIVERIES_ACCEPTED = 'deliveries_accepted';
export const ASSERTION_EFFECT_ONCE = 'business_effect_applied_once';

/**
 * Resolves the demo receiver base URL from configuration and refuses
 * anything that is not local. No request/job input ever influences this.
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

function describeError(error: unknown): string {
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

/**
 * duplicate_delivery: send the same event ID `deliveryCount` times to the
 * fixed configured demo receiver, record each attempt, then evaluate:
 *
 *  - delivery_attempt_count      -> exactly deliveryCount attempts recorded
 *  - deliveries_accepted         -> every delivery was answered with 2xx
 *  - business_effect_applied_once-> the receiver applied exactly 1 effect
 *
 * Assertion mismatches produce `failed`. Problems querying the oracle or
 * invalid oracle payloads throw, which the lifecycle reports as `error`.
 */
@Injectable()
export class DuplicateDeliveryExecutor implements ScenarioExecutor, OnModuleInit {
  readonly scenario = 'duplicate_delivery';
  private readonly logger = new Logger(DuplicateDeliveryExecutor.name);

  constructor(private readonly registry: ExecutorRegistry) {}

  onModuleInit(): void {
    this.registry.register(this);
  }

  async execute(context: ExecutionContext): Promise<ExecutionResult> {
    const receiver = resolveDemoReceiverBaseUrl();
    const deliveryUrl = new URL(DELIVERIES_PATH, receiver).toString();

    // One stable payload: every delivery must carry the SAME event ID.
    const payload = {
      eventId: context.params.eventId,
      eventType: context.params.eventType,
      orderId: context.params.orderId,
      amount: AMOUNT_MINOR_UNITS,
      currency: CURRENCY,
      occurredAt: new Date().toISOString(),
    };

    let deliveredCount = 0;
    let attemptsRecorded = 0;
    for (let attemptNumber = 1; attemptNumber <= context.params.deliveryCount; attemptNumber++) {
      const startedAt = Date.now();
      let httpStatus: number | undefined;
      let outcome: 'delivered' | 'error' | 'timeout' = 'delivered';
      let errorDetail: string | undefined;

      try {
        const response = await fetch(deliveryUrl, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(payload),
          signal: AbortSignal.timeout(context.params.timeoutMs),
        });
        httpStatus = response.status;
        if (response.ok) {
          deliveredCount += 1;
        } else {
          outcome = 'error';
          errorDetail = `Receiver responded with HTTP ${response.status}`;
        }
      } catch (error) {
        const detail = describeError(error);
        outcome = detail === 'timeout' ? 'timeout' : 'error';
        errorDetail =
          outcome === 'timeout' ? `No response within ${context.params.timeoutMs} ms` : detail;
        this.logger.warn(`Attempt ${attemptNumber} failed (${outcome}): ${errorDetail}`);
      }

      await context.report.attempt({
        attemptNumber,
        eventId: context.params.eventId,
        httpStatus,
        outcome,
        durationMs: Date.now() - startedAt,
        errorDetail: errorDetail?.slice(0, 200),
      });
      attemptsRecorded += 1;
    }

    const businessEffectCount = await this.fetchBusinessEffectCount(
      receiver,
      context.params.orderId,
      context.params.timeoutMs,
    );

    const assertions: AssertionReport[] = [
      {
        name: ASSERTION_ATTEMPT_COUNT,
        expected: String(context.params.deliveryCount),
        actual: String(attemptsRecorded),
        passed: attemptsRecorded === context.params.deliveryCount,
      },
      {
        name: ASSERTION_DELIVERIES_ACCEPTED,
        expected: String(context.params.deliveryCount),
        actual: String(deliveredCount),
        passed: deliveredCount === context.params.deliveryCount,
      },
      {
        name: ASSERTION_EFFECT_ONCE,
        expected: '1',
        actual: String(businessEffectCount),
        passed: businessEffectCount === 1,
      },
    ];
    for (const assertion of assertions) {
      await context.report.assertion(assertion);
    }

    const failed = assertions.filter((a) => !a.passed);
    if (failed.length > 0) {
      return {
        status: 'failed',
        summary: failed
          .map((a) => `${a.name} expected ${a.expected}, got ${a.actual}`)
          .join('; '),
      };
    }
    return {
      status: 'passed',
      summary: `${context.params.deliveryCount} attempts recorded, ${deliveredCount} accepted, ${businessEffectCount} business effect`,
    };
  }

  /**
   * Reads the receiver's minimum test-oracle state. A 404 means the order
   * never existed (0 effects) and is an assertion failure, NOT an error.
   * Network failures or invalid payloads are execution errors.
   */
  private async fetchBusinessEffectCount(
    receiver: URL,
    orderId: string,
    timeoutMs: number,
  ): Promise<number> {
    const oracleUrl = new URL(
      `${ORACLE_PATH_PREFIX}${encodeURIComponent(orderId)}`,
      receiver,
    ).toString();

    let response: Response;
    try {
      response = await fetch(oracleUrl, { signal: AbortSignal.timeout(timeoutMs) });
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
}
