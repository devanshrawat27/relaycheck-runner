import { Injectable, Logger } from '@nestjs/common';
import { ControlApiClient, FinalizeReport } from '../control-api/control-api.client';
import { ExecutorRegistry } from './executor-registry';
import { RunJob } from '../jobs/run-job';

/**
 * Drives one run through its lifecycle:
 *
 *   claim (queued -> running, once)  ->  execute  ->  finalize (terminal)
 *
 * A claim conflict (redelivered job / another worker) stops immediately —
 * duplicate jobs never start concurrent execution. Unexpected execution
 * failures finalize the run as `error`, never as `failed`.
 */
@Injectable()
export class LifecycleService {
  private readonly logger = new Logger(LifecycleService.name);

  constructor(
    private readonly client: ControlApiClient,
    private readonly registry: ExecutorRegistry,
  ) {}

  async process(job: RunJob): Promise<void> {
    let claim: 'claimed' | 'conflict';
    try {
      claim = await this.client.claim(job.runId);
    } catch (error) {
      this.logger.error(`Could not claim run ${job.runId}: ${safeMessage(error)}`);
      return;
    }
    if (claim === 'conflict') {
      this.logger.warn(`Run ${job.runId} is not claimable (already running/finished); duplicate job ignored`);
      return;
    }

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
      await this.finalize(job.runId, { status: result.status, summary: result.summary });
    } catch (error) {
      const message = safeMessage(error);
      this.logger.error(`Scenario execution failed for run ${job.runId}: ${message}`);
      await this.finalize(job.runId, {
        status: 'error',
        summary: 'Scenario execution could not complete',
        safeError: message.slice(0, 200),
      });
    }
  }

  /** Bounded retry: a transient Control API hiccup should not strand a run while we hold the message. */
  private async finalize(runId: string, report: FinalizeReport): Promise<void> {
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        await this.client.finalize(runId, report);
        return;
      } catch (error) {
        if (attempt === 2) {
          this.logger.error(`Could not finalize run ${runId}: ${safeMessage(error)}`);
        }
      }
    }
  }
}

function safeMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Unknown error';
}
