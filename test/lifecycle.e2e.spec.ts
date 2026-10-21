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
import { RUN_JOBS_QUEUE, RunJobConsumer } from '../src/jobs/run-job.consumer';

interface RecordedCall {
  path: string;
  body: unknown;
  token: string | undefined;
}

const AMQP = 'amqp://guest:guest@localhost:5672';
const TOKEN = 'test-internal-token';

describe('Runner worker lifecycle (e2e)', () => {
  jest.setTimeout(60000);

  let app: INestApplicationContext;
  let consumer: RunJobConsumer;
  let registry: ExecutorRegistry;
  let fakeApi: Server;
  let calls: RecordedCall[];
  const behavior = { claimStatus: 201, finalizeStatus: 201 };
  let pubConn: ChannelModel;

  function fakeControlApi(req: IncomingMessage, res: ServerResponse): void {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => {
      const rawBody = Buffer.concat(chunks).toString();
      const body: unknown = rawBody ? JSON.parse(rawBody) : undefined;
      calls.push({
        path: req.url ?? '',
        body,
        token: req.headers['x-internal-token'] as string | undefined,
      });

      res.setHeader('content-type', 'application/json');
      res.setHeader('connection', 'close');
      if (req.url?.endsWith('/claim')) {
        res.statusCode = behavior.claimStatus;
        res.end(JSON.stringify({ runId: 'run', status: behavior.claimStatus === 201 ? 'running' : 'failed' }));
        return;
      }
      if (req.url?.endsWith('/finalize')) {
        res.statusCode = behavior.finalizeStatus;
        res.end(JSON.stringify({ runId: 'run', status: 'ok' }));
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
      channel.sendToQueue(RUN_JOBS_QUEUE, Buffer.from(JSON.stringify(raw)), { persistent: true });
    } finally {
      await channel.close();
    }
  }

  async function purgeQueue(): Promise<void> {
    const channel = await pubConn.createChannel();
    try {
      await channel.assertQueue(RUN_JOBS_QUEUE, { durable: true });
      await channel.purgeQueue(RUN_JOBS_QUEUE);
    } finally {
      await channel.close();
    }
  }

  async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 15000): Promise<void> {
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
      },
      ...overrides,
    };
  }

  const registerFake = (executor: ScenarioExecutor): void => {
    registry.register(executor);
  };

  beforeAll(async () => {
    fakeApi = createServer(fakeControlApi);
    await new Promise<void>((resolve) => fakeApi.listen(0, '127.0.0.1', resolve));
    const port = (fakeApi.address() as AddressInfo).port;
    process.env.CONTROL_API_URL = `http://127.0.0.1:${port}`;
    process.env.INTERNAL_API_TOKEN = TOKEN;
    process.env.AMQP_URL = AMQP;

    pubConn = await connect(AMQP);
    // Drain any jobs left by earlier smoke runs BEFORE the consumer starts,
    // otherwise they would be prefetched and pollute the first test.
    await purgeQueue();

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
    behavior.claimStatus = 201;
    behavior.finalizeStatus = 201;
    await purgeQueue();
  });

  it('claims once, executes, records evidence, and finalizes passed', async () => {
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
    await waitFor(() => calls.some((c) => c.path.endsWith('/finalize')));

    const claimCall = calls.find((c) => c.path.includes(job.runId) && c.path.endsWith('/claim'));
    expect(claimCall?.path).toContain(job.runId);
    expect(claimCall?.token).toBe(TOKEN);
    expect(calls.some((c) => c.path.includes(job.runId) && c.path.endsWith('/attempts'))).toBe(true);
    expect(calls.some((c) => c.path.includes(job.runId) && c.path.endsWith('/assertions'))).toBe(true);

    const finalizeCall = calls.find((c) => c.path.includes(job.runId) && c.path.endsWith('/finalize'));
    expect(finalizeCall?.body).toEqual({ status: 'passed', summary: 'All assertions passed' });
  });

  it('finalizes failed when an executor reports an assertion failure', async () => {
    registerFake({
      scenario: 'test_failing',
      execute: async () => ({ status: 'failed', summary: 'business effect count expected 1, got 3' }),
    });

    await publish(makeJob({ scenario: 'test_failing' }));
    await waitFor(() => calls.some((c) => c.path.endsWith('/finalize')));

    const finalize = calls.find((c) => c.path.endsWith('/finalize'));
    expect(finalize?.body).toEqual({
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
    await waitFor(() => calls.some((c) => c.path.endsWith('/finalize')));

    const finalize = calls.find((c) => c.path.endsWith('/finalize'));
    const body = finalize?.body as { status: string; summary: string; safeError?: string };
    expect(body.status).toBe('error');
    expect(body.summary).toContain('could not complete');
    expect(body.safeError).toContain('receiver connection reset');
    expect(body.safeError).not.toContain('at ');
  });

  it('finalizes error when no executor is registered for the scenario', async () => {
    await publish(makeJob({ scenario: 'scenario_without_executor' }));
    await waitFor(() => calls.some((c) => c.path.endsWith('/finalize')));

    const body = calls.find((c) => c.path.endsWith('/finalize'))?.body as {
      status: string;
      safeError?: string;
    };
    expect(body.status).toBe('error');
    expect(body.safeError).toContain('No executor registered');
  });

  it('ignores duplicate deliveries when the claim conflicts (no second execution)', async () => {
    let executions = 0;
    registerFake({
      scenario: 'test_conflict',
      execute: async () => {
        executions += 1;
        return { status: 'passed', summary: 'done' };
      },
    });

    behavior.claimStatus = 409;
    const job = makeJob({ scenario: 'test_conflict' });
    await publish(job);
    await publish(job);

    await waitFor(() => calls.filter((c) => c.path.endsWith('/claim')).length >= 2);
    await new Promise((resolve) => setTimeout(resolve, 500));

    expect(executions).toBe(0);
    expect(calls.some((c) => c.path.endsWith('/finalize'))).toBe(false);
  });

  it('discards malformed jobs without claiming or requeueing', async () => {
    const channel = await pubConn.createChannel();
    try {
      await channel.assertQueue(RUN_JOBS_QUEUE, { durable: true });
      channel.sendToQueue(RUN_JOBS_QUEUE, Buffer.from('not-json'));
      channel.sendToQueue(RUN_JOBS_QUEUE, Buffer.from(JSON.stringify({ schemaVersion: 99, runId: '' })));
    } finally {
      await channel.close();
    }

    await waitFor(async () => {
      const check = await pubConn.createChannel();
      try {
        const info = await check.checkQueue(RUN_JOBS_QUEUE);
        return info.messageCount === 0;
      } finally {
        await check.close();
      }
    });
    expect(calls.filter((c) => c.path.endsWith('/claim'))).toHaveLength(0);
  });
});
