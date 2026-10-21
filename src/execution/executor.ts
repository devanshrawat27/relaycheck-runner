import { RunJobParams } from '../jobs/run-job';

export interface AttemptReport {
  attemptNumber: number;
  eventId: string;
  httpStatus?: number;
  outcome: 'delivered' | 'error' | 'timeout';
  durationMs: number;
  errorDetail?: string;
}

export interface AssertionReport {
  name: string;
  expected: string;
  actual: string;
  passed: boolean;
}

/** Reporting channel handed to executors; routes through the Control API. */
export interface ExecutionReporter {
  attempt(report: AttemptReport): Promise<void>;
  assertion(report: AssertionReport): Promise<void>;
}

export interface ExecutionContext {
  runId: string;
  scenario: string;
  params: RunJobParams;
  report: ExecutionReporter;
}

export interface ExecutionResult {
  status: 'passed' | 'failed';
  summary: string;
}

/** A scenario executor owns bounded delivery + assertion logic for one scenario. */
export interface ScenarioExecutor {
  readonly scenario: string;
  execute(context: ExecutionContext): Promise<ExecutionResult>;
}
