import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ExecutorRegistry } from '../execution/executor-registry';
import { ExecutionContext, ExecutionResult, ScenarioExecutor } from '../execution/executor';

/** Fixed delivery path on the demo receiver; the host comes only from configuration. */
const DELIVERIES_PATH = '/api/webhooks/demo';
const DEFAULT_RECEIVER_URL = 'http://localhost:3100';
const AMOUNT_MINOR_UNITS = 5000;
const CURRENCY = 'USD';
const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '::1']);

/**
 * Resolves the scenario target from configuration and refuses anything that
 * is not the local demo receiver. No request/job input ever influences this.
 */
export function resolveDemoReceiverUrl(): URL {
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
  url.pathname = DELIVERIES_PATH;
  url.search = '';
  url.hash = '';
  return url;
}

/**
 * duplicate_delivery: send the same event ID `deliveryCount` times to the
 * fixed configured demo receiver, recording each attempt through the
 * Control API.
 *
 * The verdict (attempt-count / business-effect assertions) belongs to the
 * assertion task; until it lands this executor withholds a verdict and
 * fails loudly instead of pretending the run passed.
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
    const endpoint = resolveDemoReceiverUrl().toString();

    // One stable payload: every delivery must carry the SAME event ID.
    const payload = {
      eventId: context.params.eventId,
      eventType: context.params.eventType,
      orderId: context.params.orderId,
      amount: AMOUNT_MINOR_UNITS,
      currency: CURRENCY,
      occurredAt: new Date().toISOString(),
    };

    for (let attemptNumber = 1; attemptNumber <= context.params.deliveryCount; attemptNumber++) {
      const startedAt = Date.now();
      let httpStatus: number | undefined;
      let outcome: 'delivered' | 'error' | 'timeout' = 'delivered';
      let errorDetail: string | undefined;

      try {
        const response = await fetch(endpoint, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(payload),
          signal: AbortSignal.timeout(context.params.timeoutMs),
        });
        httpStatus = response.status;
        if (!response.ok) {
          outcome = 'error';
          errorDetail = `Receiver responded with HTTP ${response.status}`;
        }
      } catch (error) {
        // AbortSignal.timeout rejects with a DOMException, which is not an
        // `instanceof Error` in Node — classify by name, not by class.
        const name =
          typeof error === 'object' && error !== null && 'name' in error
            ? String((error as { name: unknown }).name)
            : '';
        const isTimeout = name === 'TimeoutError' || name === 'AbortError';
        outcome = isTimeout ? 'timeout' : 'error';
        errorDetail = isTimeout
          ? `No response within ${context.params.timeoutMs} ms`
          : error instanceof Error
            ? error.message
            : 'Network error';
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
    }

    throw new Error('Assertion evaluation is not implemented yet; verdict withheld');
  }
}
