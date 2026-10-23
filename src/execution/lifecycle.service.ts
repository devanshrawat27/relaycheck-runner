import { randomUUID } from 'crypto';
import { Injectable, Logger } from '@nestjs/common';
import {
  ClaimOutcome,
  ControlApiClient,
  ControlApiError,
  FinalizeReport,
} from '../control-api/control-api.client';
import { RecordedAttempt } from './executor';
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
 *   claim (queued -> running, lease owned by this worker) -> execute -> finalize
 *
 * Ownership is lease-based: while executing, the worker heartbeats. A
 * duplicate job may only recover a run whose lease has actually expired —
 * an actively executing worker is never prematurely failed.
 *
 * Failure policy (returned to the consumer, which owns ack/requeue):
 *  - retryable Control API problems (network/5xx)  -> retry the job message
 *  - permanent problems (401/403/404)              -> dead-letter the job
 *  - duplicate job for an active (unexpired) lease -> ignore (ack)
 *  - duplicate job for an expired lease            -> recover to `error`
 *  - execution failure                             -> run finalized as `error`
 */
@Injectable()
export class LifecycleService {
  private readonly logger = new Logger(LifecycleService.name);
  private readonly workerId = `runner-${randomUUID()}`;
  private readonly heartbeatMs = Number(process.env.RUNNER_HEARTBEAT_MS ?? 10000);

  constructor(
    private readonly client: ControlApiClient,
    private readonly registry: ExecutorRegistry,
  ) {}

  async process(job: RunJob): Promise<ProcessOutcome> {
    const claim = await this.claimWithRetry(job.runId);
    if ('action' in claim) {
      return claim;
    }
    if (claim.outcome === 'conflict') {
      return this.handleClaimConflict(job, claim);
    }

    return this.executeRun(job);
  }

  /** Execute the scenario (optionally resuming recorded attempts) and finalize. */
  private async executeRun(
    job: RunJob,
    resumeAttempts?: RecordedAttempt[],
  ): Promise<ProcessOutcome> {
    let finalReport: FinalizeReport;
    const heartbeat = setInterval(() => {
      void this.client
        .heartbeat(job.runId, this.workerId)
        .then((owned) => {
          if (!owned) {
            this.logger.warn(`Lost lease ownership for run ${job.runId}`);
          }
        })
        .catch((error: unknown) => {
          this.logger.warn(`Heartbeat failed for run ${job.runId}: ${safeMessage(error)}`);
        });
    }, this.heartbeatMs);

    try {
      const executor = this.registry.get(job.scenario);
      if (!executor) {
        throw new Error(`No executor registered for scenario "${job.scenario}"`);
      }
      const result = await executor.execute({
        runId: job.runId,
        scenario: job.scenario,
        params: job.params,
        resumeAttempts,
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
    } finally {
      clearInterval(heartbeat);
    }

    return this.finalizeWithRetry(job.runId, finalReport);
  }

  private async claimWithRetry(
    runId: string,
  ): Promise<ClaimOutcome | { action: 'retry' | 'dead-letter'; reason: string }> {
    for (let attempt = 1; attempt <= CLAIM_ATTEMPTS; attempt++) {
      try {
        return await this.client.claim(runId, this.workerId);
      } catch (error) {
        const retryable = error instanceof ControlApiError ? error.retryable : true;
        if (!retryable) {
          this.logger.error(`Permanent claim failure for run ${runId}: ${safeMessage(error)}`);
          return { action: 'dead-letter', reason: safeMessage(error) };
        }
        if (attempt === CLAIM_ATTEMPTS) {
          this.logger.error(
            `Could not claim run ${runId} after ${CLAIM_ATTEMPTS} attempts: ${safeMessage(error)}`,
          );
          return { action: 'retry', reason: safeMessage(error) };
        }
        await sleep(200 * attempt);
      }
    }
    return { action: 'retry', reason: 'claim retries exhausted' };
  }

  /**
   * A duplicate job found the run claimed by someone else. Only an expired
   * lease is grounds for recovery; an active lease means it is still being
   * executed and must be left alone.
   */
  private async handleClaimConflict(
    job: RunJob,
    conflict: Extract<ClaimOutcome, { outcome: 'conflict' }>,
  ): Promise<ProcessOutcome> {
    const runId = job.runId;
    if (TERMINAL_STATUSES.has(conflict.status)) {
      this.logger.warn(`Duplicate job for finished run ${runId} (${conflict.status}); ignoring`);
      return { action: 'ack' };
    }
    if (conflict.status !== 'running') {
      this.logger.warn(`Duplicate job for run ${runId} in status ${conflict.status}; ignoring`);
      return { action: 'ack' };
    }

    const expiresAt = conflict.leaseExpiresAt ? Date.parse(conflict.leaseExpiresAt) : NaN;
    const leaseActive = Number.isFinite(expiresAt) && expiresAt > Date.now();
    if (leaseActive) {
      this.logger.warn(
        `Duplicate job for run ${runId}: lease is still active (owner executing); ignoring`,
      );
      return { action: 'ack' };
    }

    try {
      const resumed = await this.client.resumeClaim(runId, this.workerId);
      if (!resumed) {
        this.logger.warn(`Run ${runId} could not be resumed (claimed or finished elsewhere)`);
        return { action: 'ack' };
      }
      const progress = await this.client.getProgress(runId);
      this.logger.warn(
        `Resuming run ${runId} from ${progress.attempts.length} recorded attempt(s)`,
      );
      return this.executeRun(job, progress.attempts);
    } catch (error) {
      const retryable = error instanceof ControlApiError ? error.retryable : true;
      this.logger.error(`Resume failed for run ${runId}: ${safeMessage(error)}`);
      return retryable
        ? { action: 'retry', reason: safeMessage(error) }
        : { action: 'dead-letter', reason: safeMessage(error) };
    }
  }

  private async finalizeWithRetry(runId: string, report: FinalizeReport): Promise<ProcessOutcome> {
    for (let attempt = 1; attempt <= FINALIZE_ATTEMPTS; attempt++) {
      try {
        await this.client.finalize(runId, report);
        return { action: 'ack' };
      } catch (error) {
        if (error instanceof ControlApiError && error.status === 409) {
          this.logger.warn(`Run ${runId} is already terminal; ignoring finalize conflict`);
          return { action: 'ack' };
        }
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

function safeMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Unknown error';
}

