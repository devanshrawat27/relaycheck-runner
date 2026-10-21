import { Injectable } from '@nestjs/common';
import { AssertionReport, AttemptReport } from '../execution/executor';

export type ClaimResult = 'claimed' | 'conflict';

export interface FinalizeReport {
  status: 'passed' | 'failed' | 'error';
  summary: string;
  safeError?: string;
}

/**
 * Typed Control API failure so callers can distinguish transient problems
 * (network, 5xx → retry) from permanent ones (401/403/404/409 → do not loop).
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
 * these authenticated calls.
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

  /** Claim a run exactly once. A conflict means another worker owns it. */
  async claim(runId: string): Promise<ClaimResult> {
    const response = await this.request('POST', `/api/internal/runs/${runId}/claim`);
    if (response.status === 201 || response.status === 200) {
      return 'claimed';
    }
    if (response.status === 409) {
      return 'conflict';
    }
    throw new ControlApiError(
      `Claim failed with HTTP ${response.status}`,
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

  /** Run status for duplicate-job recovery decisions; null when it no longer exists. */
  async getRunStatus(runId: string): Promise<string | null> {
    const response = await this.request('GET', `/api/runs/${runId}`);
    if (response.status === 404) {
      return null;
    }
    if (!response.ok) {
      throw new ControlApiError(
        `Run lookup failed with HTTP ${response.status}`,
        isRetryableStatus(response.status),
        response.status,
      );
    }
    const body = (await response.json()) as { status?: unknown };
    return typeof body.status === 'string' ? body.status : null;
  }
}
