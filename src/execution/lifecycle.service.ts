import { Injectable, Logger } from '@nestjs/common';
import { ControlApiClient, ControlApiError, FinalizeReport } from '../control-api/control-api.client';
import { ExecutorRegistry } from './executor-registry';
import { RunJob } from '../jobs/run-job';

/** What the consumer should do with the message after processing. */
export type ProcessOutcome =
  | { action: 'ack' }
  | { action: 'retry'; reason: string }
  | { action: 'dead-letter'; reason: string };

const TERMINAL_STATUSES = new Set(['passed', 'failed', 'error']);
const CLAIM_ATTEMPTS = 3;
const FINALIZE_ATTEMPTS = 3;

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Drives one run through its lifecycle:
 *
 *   claim (queued -> running, once)  ->  execute  ->  finalize (terminal)
 *
 * Failure policy (returned to the consumer, which owns ack/requeue):
 *  - retryable Control API problems (network/5xx)  -> retry the job message
 *  - permanent problems (401/403/404)              -> dead-letter the job
 *  - duplicate job for an already-running run      -> recovery finalize `error`
 *  - duplicate job for a finished/deleted run      -> ack and ignore
 *  - execution failure                             -> run finalized as `error`
 */
@Injectable()
export class LifecycleService {
  private readonly logger = new Logger(LifecycleService.name);

  constructor(
    private readonly client: ControlApiClient,
    private readonly registry: ExecutorRegistry,
  ) {}

  async process(job: RunJob): Promise<ProcessOutcome> {
    const claim = await this.claimWithRetry(job.runId);
    if (claim.outcome !== 'claimed') {
      return claim.outcome === 'conflict'
        ? await this.handleClaimConflict(job.runId)
        : claim.outcome;
    }

    let finalReport: FinalizeReport;
    try {
      const executor = this.registry.get(job.scenario);
      if (!executor) {
        throw new Error(`No executor registered for scenario "${job.scenario}"`);
      }
      const result = await executor.execute({
        runId: job.runId,
        scenario: job.scenario,
        params: job.params,
        report: {
          attempt: (report) => this.client.recordAttempt(job.runId, report),
          assertion: (report) => this.client.recordAssertion(job.runId, report),
        },
      });
      finalReport = { status: result.status, summary: result.summary };
    } catch (error) {
      const message = safeMessage(error);
      this.logger.error(`Scenario execution failed for run ${job.runId}: ${message}`);
      finalReport = {
        status: 'error',
        summary: 'Scenario execution could not complete',
        safeError: message.slice(0, 200),
      };
    }

    return this.finalizeWithRetry(job.runId, finalReport);
  }

  private async claimWithRetry(
    runId: string,
  ): Promise<{ outcome: 'claimed' | 'conflict' } | { outcome: ProcessOutcome }> {
    for (let attempt = 1; attempt <= CLAIM_ATTEMPTS; attempt++) {
      try {
        const result = await this.client.claim(runId);
        return { outcome: result };
      } catch (error) {
        const retryable = error instanceof ControlApiError ? error.retryable : true;
        if (!retryable) {
          this.logger.error(`Permanent claim failure for run ${runId}: ${safeMessage(error)}`);
          return { outcome: { action: 'dead-letter', reason: safeMessage(error) } };
        }
        if (attempt === CLAIM_ATTEMPTS) {
          this.logger.error(
            `Could not claim run ${runId} after ${CLAIM_ATTEMPTS} attempts: ${safeMessage(error)}`,
          );
          return { outcome: { action: 'retry', reason: safeMessage(error) } };
        }
        await sleep(200 * attempt);
      }
    }
    return { outcome: { action: 'retry', reason: 'claim retries exhausted' } };
  }

  /**
   * A redelivered job found the run already claimed. If it is still `running`,
   * a previous worker likely died mid-execution — finalize it as `error` so the
   * run cannot stay stuck forever. Terminal runs and deleted runs are ignored.
   */
  private async handleClaimConflict(runId: string): Promise<ProcessOutcome> {
    let status: string | null;
    try {
      status = await this.client.getRunStatus(runId);
    } catch (error) {
      return retryOrDeadLetter(error, runId, 'run status lookup');
    }

    if (status === null) {
      this.logger.warn(`Duplicate job for missing run ${runId}; discarding`);
      return { action: 'ack' };
    }
    if (TERMINAL_STATUSES.has(status)) {
      this.logger.warn(`Duplicate job for finished run ${runId} (${status}); ignoring`);
      return { action: 'ack' };
    }

    this.logger.warn(`Run ${runId} is still ${status}; attempting recovery finalize`);
    return this.finalizeWithRetry(runId, {
      status: 'error',
      summary: 'Execution was interrupted before completion',
      safeError: 'A duplicate job found the run still running; recovery finalized it as error',
    });
  }

  private async finalizeWithRetry(runId: string, report: FinalizeReport): Promise<ProcessOutcome> {
    for (let attempt = 1; attempt <= FINALIZE_ATTEMPTS; attempt++) {
      try {
        await this.client.finalize(runId, report);
        return { action: 'ack' };
      } catch (error) {
        const retryable = error instanceof ControlApiError ? error.retryable : true;
        if (!retryable) {
          this.logger.error(`Permanent finalize failure for run ${runId}: ${safeMessage(error)}`);
          return { action: 'dead-letter', reason: safeMessage(error) };
        }
        if (attempt === FINALIZE_ATTEMPTS) {
          this.logger.error(
            `Could not finalize run ${runId} after ${FINALIZE_ATTEMPTS} attempts: ${safeMessage(error)}`,
          );
          return { action: 'retry', reason: safeMessage(error) };
        }
        await sleep(200 * attempt);
      }
    }
    return { action: 'retry', reason: 'finalize retries exhausted' };
  }
}

function retryOrDeadLetter(error: unknown, runId: string, context: string): ProcessOutcome {
  const retryable = error instanceof ControlApiError ? error.retryable : true;
  const message = `${context} failed for run ${runId}: ${safeMessage(error)}`;
  return retryable
    ? { action: 'retry', reason: message }
    : { action: 'dead-letter', reason: message };
}

function safeMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Unknown error';
}
