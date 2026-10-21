import { Injectable } from '@nestjs/common';
import { AssertionReport, AttemptReport } from '../execution/executor';

export type ClaimResult = 'claimed' | 'conflict';

export interface FinalizeReport {
  status: 'passed' | 'failed' | 'error';
  summary: string;
  safeError?: string;
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

  private async post(path: string, body?: unknown): Promise<Response> {
    return fetch(`${this.baseUrl}${path}`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-internal-token': this.token,
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  }

  /** Claim a run exactly once. A conflict means another worker owns it. */
  async claim(runId: string): Promise<ClaimResult> {
    const response = await this.post(`/api/internal/runs/${runId}/claim`);
    if (response.status === 201 || response.status === 200) {
      return 'claimed';
    }
    if (response.status === 409) {
      return 'conflict';
    }
    throw new Error(`Claim failed with HTTP ${response.status}`);
  }

  async recordAttempt(runId: string, attempt: AttemptReport): Promise<void> {
    const response = await this.post(`/api/internal/runs/${runId}/attempts`, attempt);
    if (!response.ok) {
      throw new Error(`Recording attempt failed with HTTP ${response.status}`);
    }
  }

  async recordAssertion(runId: string, assertion: AssertionReport): Promise<void> {
    const response = await this.post(`/api/internal/runs/${runId}/assertions`, assertion);
    if (!response.ok) {
      throw new Error(`Recording assertion failed with HTTP ${response.status}`);
    }
  }

  async finalize(runId: string, report: FinalizeReport): Promise<void> {
    const response = await this.post(`/api/internal/runs/${runId}/finalize`, report);
    if (!response.ok) {
      throw new Error(`Finalize failed with HTTP ${response.status}`);
    }
  }
}
