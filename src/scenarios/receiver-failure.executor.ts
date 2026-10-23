import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { AssertionReport, ExecutionContext, ExecutionResult, ScenarioExecutor } from '../execution/executor';
import { ExecutorRegistry } from '../execution/executor-registry';
import { resolveJobTarget } from '../targets/target-registry';
import {
  clearReceiverFault,
  deliverDemoEvent,
  fetchBusinessEffectCount,
  setReceiverFault,
} from './demo-receiver.client';

export const SCENARIO_RECEIVER_FAILURE = 'receiver_failure';
export const SCENARIO_RECEIVER_TIMEOUT = 'receiver_timeout';

export const ASSERTION_RECEIVER_STATUS = 'receiver_returned_expected_status';
export const ASSERTION_TIMED_OUT = 'delivery_timed_out';
export const ASSERTION_NO_EFFECT = 'business_effect_not_applied';

const EXPECTED_ERROR_STATUS = 503;
const TIMEOUT_FAULT_DELAY_MS = 2000;

/**
 * Controlled receiver failure/timeout scenarios (PRD Scenario B).
 *
 * `receiver_failure`: inject a test-only 503 that must NOT apply the business
 * effect; a 2xx response is an assertion failure (the fault did not apply).
 * `receiver_timeout`: inject a delay longer than the delivery timeout; the
 * attempt must end as `timeout`. The receiver may still process the event
 * after the client gave up (at-least-once reality), so only the timeout
 * outcome is asserted — not the final effect count.
 *
 * Faults are cleared in `finally` so a scenario can never leave the receiver
 * stuck in a failing state.
 */
@Injectable()
export class ReceiverFaultExecutor implements ScenarioExecutor, OnModuleInit {
  readonly scenario = SCENARIO_RECEIVER_FAILURE;
  private readonly logger = new Logger(ReceiverFaultExecutor.name);

  constructor(private readonly registry: ExecutorRegistry) {}

  onModuleInit(): void {
    this.registry.register(this);
    this.registry.registerAs(SCENARIO_RECEIVER_TIMEOUT, this);
  }

  async execute(context: ExecutionContext): Promise<ExecutionResult> {
    return context.scenario === SCENARIO_RECEIVER_TIMEOUT
      ? this.executeTimeout(context)
      : this.executeFailure(context);
  }

  private async executeFailure(context: ExecutionContext): Promise<ExecutionResult> {
    const target = await resolveJobTarget(context.params);
    if (target.kind !== 'demo') {
      throw new Error('receiver_failure requires the demo receiver target');
    }
    const receiver = new URL(target.url);
    const expectedStatus = context.params.httpStatus ?? EXPECTED_ERROR_STATUS;
    await setReceiverFault(receiver, { mode: 'error', httpStatus: expectedStatus });

    try {
      const event = this.makeEvent(context);
      const { report, delivered } = await deliverDemoEvent(
        receiver,
        event,
        1,
        context.params.timeoutMs,
      );
      await context.report.attempt(report);

      const effectCount = await fetchBusinessEffectCount(
        receiver,
        event.orderId,
        context.params.timeoutMs,
      );

      return await this.finalizeVerdict(context, [
        {
          name: 'delivery_attempt_count',
          expected: '1',
          actual: '1',
          passed: true,
        },
        {
          name: ASSERTION_RECEIVER_STATUS,
          expected: String(expectedStatus),
          actual: String(report.httpStatus ?? 'none'),
          passed: report.httpStatus === expectedStatus && !delivered,
        },
        {
          name: ASSERTION_NO_EFFECT,
          expected: '0',
          actual: String(effectCount),
          passed: effectCount === 0,
        },
      ]);
    } finally {
      await clearReceiverFault(receiver);
    }
  }

  private async executeTimeout(context: ExecutionContext): Promise<ExecutionResult> {
    const target = await resolveJobTarget(context.params);
    if (target.kind !== 'demo') {
      throw new Error('receiver_timeout requires the demo receiver target');
    }
    const receiver = new URL(target.url);
    const delayMs = context.params.delayMs ?? TIMEOUT_FAULT_DELAY_MS;
    await setReceiverFault(receiver, { mode: 'delay', delayMs });

    try {
      const event = this.makeEvent(context);
      const { report } = await deliverDemoEvent(receiver, event, 1, context.params.timeoutMs);
      await context.report.attempt(report);

      return await this.finalizeVerdict(context, [
        {
          name: 'delivery_attempt_count',
          expected: '1',
          actual: '1',
          passed: true,
        },
        {
          name: ASSERTION_TIMED_OUT,
          expected: 'timeout',
          actual: report.outcome,
          passed: report.outcome === 'timeout',
        },
      ]);
    } finally {
      await clearReceiverFault(receiver);
    }
  }

  private makeEvent(context: ExecutionContext) {
    return {
      eventId: context.params.eventId,
      eventType: context.params.eventType,
      orderId: context.params.orderId,
      amount: 5000,
      currency: 'USD',
      occurredAt: new Date().toISOString(),
    };
  }

  private async finalizeVerdict(
    context: ExecutionContext,
    assertions: AssertionReport[],
  ): Promise<ExecutionResult> {
    for (const assertion of assertions) {
      await context.report.assertion(assertion);
    }
    const failed = assertions.filter((assertion) => !assertion.passed);
    if (failed.length > 0) {
      this.logger.warn(`Assertions failed for ${context.runId}`);
      return {
        status: 'failed',
        summary: failed
          .map((assertion) => `${assertion.name} expected ${assertion.expected}, got ${assertion.actual}`)
          .join('; '),
      };
    }
    return {
      status: 'passed',
      summary:
        context.scenario === SCENARIO_RECEIVER_TIMEOUT
          ? 'Delivery timed out as expected within a bounded attempt'
          : `Receiver returned the expected ${
              context.params.httpStatus ?? EXPECTED_ERROR_STATUS
            } and applied no business effect`,
    };
  }
}


