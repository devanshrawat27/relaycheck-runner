import 'dotenv/config';
import { createServer, IncomingMessage, Server, ServerResponse } from 'http';
import { AddressInfo } from 'net';
import { INestApplicationContext } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { connect } from 'amqplib';
import type { ChannelModel } from 'amqplib';
import { AppModule } from '../src/app.module';
import { ExecutorRegistry } from '../src/execution/executor-registry';
import { ScenarioExecutor } from '../src/execution/executor';
import { RUN_JOBS_DLQ, RUN_JOBS_QUEUE, RunJobConsumer } from '../src/jobs/run-job.consumer';

interface RecordedCall {
  method: string;
  path: string;
  body: unknown;
  token: string | undefined;
}

const AMQP = 'amqp://guest:guest@localhost:5672';
const TOKEN = 'test-internal-token';

describe('Runner worker lifecycle (e2e)', () => {
  jest.setTimeout(90000);

  let app: INestApplicationContext;
  let consumer: RunJobConsumer;
  let registry: ExecutorRegistry;
  let fakeApi: Server;
  let calls: RecordedCall[];
  const behavior = {
    claimDefault: 201,
    claimStatuses: [] as number[],
    finalizeDefault: 201,
    finalizeStatuses: [] as number[],
    runStatus: 'running',
    leaseExpiresAt: null as string | null,
    recoverDefault: 201,
  };
  let pubConn: ChannelModel;

  function nextStatus(sequence: number[], fallback: number): number {
    const next = sequence.shift();
    return next ?? fallback;
  }

  function fakeControlApi(req: IncomingMessage, res: ServerResponse): void {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => {
      const rawBody = Buffer.concat(chunks).toString();
      const body: unknown = rawBody ? JSON.parse(rawBody) : undefined;
      calls.push({
        method: req.method ?? '',
        path: req.url ?? '',
        body,
        token: req.headers['x-internal-token'] as string | undefined,
      });

      res.setHeader('content-type', 'application/json');
      res.setHeader('connection', 'close');

      if (req.url?.endsWith('/claim')) {
        const status = nextStatus(behavior.claimStatuses, behavior.claimDefault);
        res.statusCode = status;
        if (status === 201) {
          res.end(
            JSON.stringify({
              runId: 'run',
              status: 'running',
              leaseExpiresAt:
                behavior.leaseExpiresAt ?? new Date(Date.now() + 30_000).toISOString(),
            }),
          );
        } else {
          res.end(
            JSON.stringify({
              status: behavior.runStatus,
              leaseExpiresAt: behavior.leaseExpiresAt,
              message: 'conflict',
            }),
          );
        }
        return;
      }

      if (req.url?.endsWith('/heartbeat')) {
        res.statusCode = 201;
        res.end(JSON.stringify({ extended: true }));
        return;
      }

      if (req.url?.endsWith('/recover')) {
        res.statusCode = behavior.recoverDefault;
        res.end(JSON.stringify({ recovered: behavior.recoverDefault === 201 }));
        return;
      }

      if (req.url?.endsWith('/finalize')) {
        res.statusCode = nextStatus(behavior.finalizeStatuses, behavior.finalizeDefault);
        res.end(JSON.stringify({ ok: true }));
        return;
      }
      res.statusCode = 201;
      res.end(JSON.stringify({ ok: true }));
    });
  }

  async function publish(raw: unknown): Promise<void> {
    const channel = await pubConn.createChannel();
    try {
      await channel.assertQueue(RUN_JOBS_QUEUE, { durable: true });
      channel.sendToQueue(RUN_JOBS_QUEUE, Buffer.from(typeof raw === 'string' ? raw : JSON.stringify(raw)), {
        persistent: true,
      });
    } finally {
      await channel.close();
    }
  }

  async function queueDepth(queue: string): Promise<number> {
    const channel = await pubConn.createChannel();
    try {
      await channel.assertQueue(queue, { durable: true });
      const info = await channel.checkQueue(queue);
      return info.messageCount;
    } finally {
      await channel.close();
    }
  }

  async function drain(queue: string): Promise<void> {
    const channel = await pubConn.createChannel();
    try {
      await channel.assertQueue(queue, { durable: true });
      await channel.purgeQueue(queue);
    } finally {
      await channel.close();
    }
  }

  async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 30000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (await predicate()) {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    throw new Error('Timed out waiting for condition');
  }

  function makeJob(overrides: Record<string, unknown> = {}) {
    return {
      schemaVersion: 1,
      runId: `run_${Math.random().toString(36).slice(2)}`,
      scenario: 'test_success',
      params: {
        eventType: 'payment.succeeded',
        eventId: 'evt_runner_test_1',
        orderId: 'ord_runner_test_1',
        deliveryCount: 3,
        timeoutMs: 5000,
        targetName: 'demo-receiver',
        targetUrl: 'http://127.0.0.1:3100',
        targetOracle: true,
      },
      ...overrides,
    };
  }

  const registerFake = (executor: ScenarioExecutor): void => {
    registry.register(executor);
  };

  const callsTo = (suffix: string): RecordedCall[] => calls.filter((c) => c.path.endsWith(suffix));

  beforeAll(async () => {
    // Fast heartbeat so the heartbeat test does not take minutes.
    process.env.RUNNER_HEARTBEAT_MS = '100';

    fakeApi = createServer(fakeControlApi);
    await new Promise<void>((resolve) => fakeApi.listen(0, '127.0.0.1', resolve));
    const port = (fakeApi.address() as AddressInfo).port;
    process.env.CONTROL_API_URL = `http://127.0.0.1:${port}`;
    process.env.INTERNAL_API_TOKEN = TOKEN;
    process.env.AMQP_URL = AMQP;

    pubConn = await connect(AMQP);
    await drain(RUN_JOBS_QUEUE);
    await drain(RUN_JOBS_DLQ);

    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    await moduleRef.init();
    app = moduleRef;
    registry = app.get(ExecutorRegistry);
    consumer = app.get(RunJobConsumer);
    await consumer.start();
  });

  afterAll(async () => {
    await consumer?.stop();
    await app?.close();
    await pubConn?.close();
    fakeApi?.closeAllConnections();
    await new Promise<void>((resolve) => (fakeApi ? fakeApi.close(() => resolve()) : resolve()));
  });

  beforeEach(async () => {
    calls = [];
    behavior.claimDefault = 201;
    behavior.claimStatuses = [];
    behavior.finalizeDefault = 201;
    behavior.finalizeStatuses = [];
    behavior.runStatus = 'running';
    behavior.leaseExpiresAt = null;
    behavior.recoverDefault = 201;
    await drain(RUN_JOBS_QUEUE);
    await drain(RUN_JOBS_DLQ);
  });

  it('claims with a lease, executes, records evidence, and finalizes passed', async () => {
    registerFake({
      scenario: 'test_success',
      execute: async (context) => {
        await context.report.attempt({
          attemptNumber: 1,
          eventId: context.params.eventId,
          httpStatus: 200,
          outcome: 'delivered',
          durationMs: 12,
        });
        await context.report.assertion({
          name: 'business_effect_applied_once',
          expected: '1',
          actual: '1',
          passed: true,
        });
        return { status: 'passed', summary: 'All assertions passed' };
      },
    });

    const job = makeJob();
    await publish(job);
    await waitFor(() => callsTo('/finalize').length > 0);

    const claimCall = calls.find((c) => c.path.includes(job.runId) && c.path.endsWith('/claim'));
    expect(claimCall?.token).toBe(TOKEN);
    expect((claimCall?.body as { workerId?: string }).workerId).toMatch(/^runner-/);
    expect(calls.some((c) => c.path.includes(job.runId) && c.path.endsWith('/attempts'))).toBe(true);
    expect(calls.find((c) => c.path.includes(job.runId) && c.path.endsWith('/finalize'))?.body).toEqual({
      status: 'passed',
      summary: 'All assertions passed',
    });
    await waitFor(async () => (await queueDepth(RUN_JOBS_QUEUE)) === 0);
  });

  it('heartbeats while the executor is running', async () => {
    registerFake({
      scenario: 'test_heartbeat',
      execute: async () => {
        await new Promise((resolve) => setTimeout(resolve, 350));
        return { status: 'passed', summary: 'long enough' };
      },
    });

    await publish(makeJob({ scenario: 'test_heartbeat' }));
    await waitFor(() => callsTo('/finalize').length > 0);
    expect(callsTo('/heartbeat').length).toBeGreaterThanOrEqual(2);
  });

  it('finalizes failed when an executor reports an assertion failure', async () => {
    registerFake({
      scenario: 'test_failing',
      execute: async () => ({ status: 'failed', summary: 'business effect count expected 1, got 3' }),
    });

    await publish(makeJob({ scenario: 'test_failing' }));
    await waitFor(() => callsTo('/finalize').length > 0);

    expect(callsTo('/finalize')[0]?.body).toEqual({
      status: 'failed',
      summary: 'business effect count expected 1, got 3',
    });
  });

  it('finalizes error (not failed) when execution throws unexpectedly', async () => {
    registerFake({
      scenario: 'test_throwing',
      execute: async () => {
        throw new Error('receiver connection reset');
      },
    });

    await publish(makeJob({ scenario: 'test_throwing' }));
    await waitFor(() => callsTo('/finalize').length > 0);

    const body = callsTo('/finalize')[0]?.body as {
      status: string;
      summary: string;
      safeError?: string;
    };
    expect(body.status).toBe('error');
    expect(body.summary).toContain('could not complete');
    expect(body.safeError).toContain('receiver connection reset');
  });

  it('finalizes error when no executor is registered for the scenario', async () => {
    await publish(makeJob({ scenario: 'scenario_without_executor' }));
    await waitFor(() => callsTo('/finalize').length > 0);

    const body = callsTo('/finalize')[0]?.body as { status: string; safeError?: string };
    expect(body.status).toBe('error');
    expect(body.safeError).toContain('No executor registered');
  });

  it('ignores duplicate jobs for a finished run (no recovery, no execution)', async () => {
    let executions = 0;
    registerFake({
      scenario: 'test_conflict',
      execute: async () => {
        executions += 1;
        return { status: 'passed', summary: 'done' };
      },
    });

    behavior.claimDefault = 409;
    behavior.runStatus = 'passed';
    await publish(makeJob({ scenario: 'test_conflict' }));

    await waitFor(() => callsTo('/claim').length > 0);
    await new Promise((resolve) => setTimeout(resolve, 500));

    expect(executions).toBe(0);
    expect(callsTo('/finalize')).toHaveLength(0);
    expect(callsTo('/recover')).toHaveLength(0);
    await waitFor(async () => (await queueDepth(RUN_JOBS_QUEUE)) === 0);
  });

  it('never recovers a run whose lease is still active', async () => {
    let executions = 0;
    registerFake({
      scenario: 'test_active_lease',
      execute: async () => {
        executions += 1;
        return { status: 'passed', summary: 'done' };
      },
    });

    behavior.claimDefault = 409;
    behavior.runStatus = 'running';
    behavior.leaseExpiresAt = new Date(Date.now() + 60_000).toISOString();
    await publish(makeJob({ scenario: 'test_active_lease' }));

    await waitFor(() => callsTo('/claim').length > 0);
    await new Promise((resolve) => setTimeout(resolve, 500));

    expect(executions).toBe(0);
    expect(callsTo('/recover')).toHaveLength(0);
    expect(callsTo('/finalize')).toHaveLength(0);
    await waitFor(async () => (await queueDepth(RUN_JOBS_QUEUE)) === 0);
  });

  it('recovers a run whose lease has expired', async () => {
    let executions = 0;
    registerFake({
      scenario: 'test_expired_lease',
      execute: async () => {
        executions += 1;
        return { status: 'passed', summary: 'done' };
      },
    });

    behavior.claimDefault = 409;
    behavior.runStatus = 'running';
    behavior.leaseExpiresAt = new Date(Date.now() - 60_000).toISOString();
    await publish(makeJob({ scenario: 'test_expired_lease' }));

    await waitFor(() => callsTo('/recover').length > 0);
    await new Promise((resolve) => setTimeout(resolve, 300));

    expect(executions).toBe(0);
    expect(callsTo('/recover').length).toBeGreaterThanOrEqual(1);
    await waitFor(async () => (await queueDepth(RUN_JOBS_QUEUE)) === 0);
  });

  it('retries transient claim failures instead of losing the job', async () => {
    registerFake({
      scenario: 'test_retry_claim',
      execute: async () => ({ status: 'passed', summary: 'recovered' }),
    });

    behavior.claimStatuses = [503, 503];
    await publish(makeJob({ scenario: 'test_retry_claim' }));

    await waitFor(() => callsTo('/finalize').length > 0);
    const finalize = callsTo('/finalize')[0]?.body as { status: string };
    expect(finalize.status).toBe('passed');
    expect(callsTo('/claim').length).toBeGreaterThanOrEqual(3);
    await waitFor(async () => (await queueDepth(RUN_JOBS_QUEUE)) === 0);
  });

  it('dead-letters a job when claim failures persist (bounded, not lost)', async () => {
    let executions = 0;
    registerFake({
      scenario: 'test_dlq',
      execute: async () => {
        executions += 1;
        return { status: 'passed', summary: 'never' };
      },
    });

    behavior.claimDefault = 503;
    await publish(makeJob({ scenario: 'test_dlq' }));

    await waitFor(async () => (await queueDepth(RUN_JOBS_DLQ)) === 1, 60000);
    expect(executions).toBe(0);
    await waitFor(async () => (await queueDepth(RUN_JOBS_QUEUE)) === 0);
  });

  it('retries a transient finalize failure and still acknowledges', async () => {
    registerFake({
      scenario: 'test_retry_finalize',
      execute: async () => ({ status: 'passed', summary: 'ok' }),
    });

    behavior.finalizeStatuses = [503];
    await publish(makeJob({ scenario: 'test_retry_finalize' }));

    await waitFor(() => callsTo('/finalize').length >= 2);
    await waitFor(async () => (await queueDepth(RUN_JOBS_QUEUE)) === 0);
    await waitFor(async () => (await queueDepth(RUN_JOBS_DLQ)) === 0);
  });

  it('discards malformed jobs to the dead-letter queue without claiming', async () => {
    await publish('not-json');
    await publish({ schemaVersion: 99, runId: '' });

    await waitFor(async () => (await queueDepth(RUN_JOBS_DLQ)) >= 2);
    expect(callsTo('/claim')).toHaveLength(0);
    await waitFor(async () => (await queueDepth(RUN_JOBS_QUEUE)) === 0);
  });
});

