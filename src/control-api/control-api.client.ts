import { Injectable } from '@nestjs/common';
import { AssertionReport, AttemptReport } from '../execution/executor';

export type ClaimOutcome =
  | { outcome: 'claimed'; leaseExpiresAt: string | null }
  | { outcome: 'conflict'; status: string; leaseExpiresAt: string | null };

export interface FinalizeReport {
  status: 'passed' | 'failed' | 'error';
  summary: string;
  safeError?: string;
}

/**
 * Typed Control API failure so callers can distinguish transient problems
 * (network, 5xx → retry) from permanent ones (401/403/404 → do not loop).
 */
export class ControlApiError extends Error {
  constructor(
    message: string,
    readonly retryable: boolean,
    readonly status?: number,
  ) {
    super(message);
    this.name = 'ControlApiError';
  }
}

function isRetryableStatus(status: number): boolean {
  return status >= 500 || status === 408 || status === 429;
}

/**
 * HTTP client for the Control API's private runner-reporting endpoints.
 * The runner never touches the platform database — every write goes through
 * these authenticated calls. Claims are lease-based so ownership is explicit.
 */
@Injectable()
export class ControlApiClient {
  private readonly baseUrl: string;
  private readonly token: string;

  constructor() {
    this.baseUrl = (process.env.CONTROL_API_URL ?? 'http://localhost:3200').replace(/\/$/, '');
    this.token = process.env.INTERNAL_API_TOKEN ?? '';
  }

  private async request(method: 'GET' | 'POST', path: string, body?: unknown): Promise<Response> {
    try {
      return await fetch(`${this.baseUrl}${path}`, {
        method,
        headers: {
          'content-type': 'application/json',
          'x-internal-token': this.token,
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    } catch (error) {
      throw new ControlApiError(
        `Control API unreachable: ${error instanceof Error ? error.message : 'network error'}`,
        true,
      );
    }
  }

  private async readJson(response: Response): Promise<Record<string, unknown>> {
    try {
      return (await response.json()) as Record<string, unknown>;
    } catch {
      return {};
    }
  }

  /** Claim a run with a worker-owned lease. A conflict means another owner (or a terminal run). */
  async claim(runId: string, workerId: string): Promise<ClaimOutcome> {
    const response = await this.request('POST', `/api/internal/runs/${runId}/claim`, { workerId });
    const body = await this.readJson(response);

    if (response.status === 201 || response.status === 200) {
      return {
        outcome: 'claimed',
        leaseExpiresAt: typeof body.leaseExpiresAt === 'string' ? body.leaseExpiresAt : null,
      };
    }
    if (response.status === 409) {
      return {
        outcome: 'conflict',
        status: typeof body.status === 'string' ? body.status : 'unknown',
        leaseExpiresAt: typeof body.leaseExpiresAt === 'string' ? body.leaseExpiresAt : null,
      };
    }
    throw new ControlApiError(
      `Claim failed with HTTP ${response.status}`,
      isRetryableStatus(response.status),
      response.status,
    );
  }

  /** Extend the lease. Returns false when this worker no longer owns the run. */
  async heartbeat(runId: string, workerId: string): Promise<boolean> {
    const response = await this.request('POST', `/api/internal/runs/${runId}/heartbeat`, { workerId });
    if (response.ok) {
      return true;
    }
    if (response.status === 409) {
      return false;
    }
    throw new ControlApiError(
      `Heartbeat failed with HTTP ${response.status}`,
      isRetryableStatus(response.status),
      response.status,
    );
  }

  /** Recover a run whose lease has expired. Returns false when not recoverable. */
  async recover(runId: string): Promise<boolean> {
    const response = await this.request('POST', `/api/internal/runs/${runId}/recover`);
    if (response.ok) {
      return true;
    }
    if (response.status === 409) {
      return false;
    }
    throw new ControlApiError(
      `Recovery failed with HTTP ${response.status}`,
      isRetryableStatus(response.status),
      response.status,
    );
  }

  async recordAttempt(runId: string, attempt: AttemptReport): Promise<void> {
    const response = await this.request('POST', `/api/internal/runs/${runId}/attempts`, attempt);
    if (!response.ok) {
      throw new ControlApiError(
        `Recording attempt failed with HTTP ${response.status}`,
        isRetryableStatus(response.status),
        response.status,
      );
    }
  }

  async recordAssertion(runId: string, assertion: AssertionReport): Promise<void> {
    const response = await this.request('POST', `/api/internal/runs/${runId}/assertions`, assertion);
    if (!response.ok) {
      throw new ControlApiError(
        `Recording assertion failed with HTTP ${response.status}`,
        isRetryableStatus(response.status),
        response.status,
      );
    }
  }

  /** Finalize the run. A 409 means it is already terminal (another path won). */
  async finalize(runId: string, report: FinalizeReport): Promise<void> {
    const response = await this.request('POST', `/api/internal/runs/${runId}/finalize`, report);
    if (!response.ok) {
      throw new ControlApiError(
        `Finalize failed with HTTP ${response.status}`,
        isRetryableStatus(response.status),
        response.status,
      );
    }
  }
}
