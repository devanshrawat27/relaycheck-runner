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

/** An attempt already recorded before an interrupted execution was resumed. */
export interface RecordedAttempt {
  attemptNumber: number;
  eventId: string;
  httpStatus?: number | null;
  outcome: 'delivered' | 'error' | 'timeout';
  durationMs: number;
  errorDetail?: string | null;
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
  /** Present when resuming an interrupted run; already-recorded attempts. */
  resumeAttempts?: RecordedAttempt[];
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
