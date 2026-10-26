import { createServer, IncomingMessage, Server, ServerResponse } from 'http';
import { AddressInfo } from 'net';
import { ExecutorRegistry } from '../src/execution/executor-registry';
import { AssertionReport, AttemptReport, ExecutionContext } from '../src/execution/executor';
import {
  ASSERTION_ATTEMPT_COUNT,
  ASSERTION_DELIVERIES_ACCEPTED,
  ASSERTION_EFFECT_ONCE,
  DuplicateDeliveryExecutor,
} from '../src/scenarios/duplicate-delivery.executor';
import { parseRunJob } from '../src/jobs/run-job';
import { isPrivateHost } from '../src/targets/target-registry';

interface RecordedRequest {
  method: string;
  path: string;
  body: Record<string, unknown> | null;
}

describe('duplicate_delivery executor (e2e)', () => {
  jest.setTimeout(30000);

  let receiver: Server;
  let receiverUrl: string;
  let requests: RecordedRequest[];
  let attempts: AttemptReport[];
  let assertions: AssertionReport[];
  const behavior = {
    status: 201,
    delayMs: 0,
    effectCount: 1,
    oracleStatus: 200,
    oracleDelayMs: 0,
    redirectTo: '' as string,
  };

  function handler(req: IncomingMessage, res: ServerResponse): void {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => {
      const bodyText = Buffer.concat(chunks).toString();
      requests.push({
        method: req.method ?? '',
        path: req.url ?? '',
        body: bodyText ? (JSON.parse(bodyText) as Record<string, unknown>) : null,
      });
      res.setHeader('content-type', 'application/json');
      res.setHeader('connection', 'close');

      if (req.method === 'GET' && req.url?.startsWith('/api/__test__/orders/')) {
        const orderId = req.url.split('/').pop() ?? '';
        const respond = (): void => {
          res.statusCode = behavior.oracleStatus;
          res.end(
            behavior.oracleStatus === 200
              ? JSON.stringify({ orderId, orderStatus: 'PAID', businessEffectCount: behavior.effectCount })
              : JSON.stringify({ message: 'oracle failure' }),
          );
        };
        if (behavior.oracleDelayMs > 0) {
          setTimeout(respond, behavior.oracleDelayMs);
        } else {
          respond();
        }
        return;
      }

      const respond = (): void => {
        if (behavior.redirectTo) {
          res.statusCode = 302;
          res.setHeader('location', behavior.redirectTo);
          res.end();
          return;
        }
        res.statusCode = behavior.status;
        res.end(JSON.stringify({ accepted: true }));
      };
      if (behavior.delayMs > 0) {
        setTimeout(respond, behavior.delayMs);
      } else {
        respond();
      }
    });
  }

  function makeContext(
    overrides: { timeoutMs?: number; deliveryCount?: number; orderId?: string } = {},
  ): ExecutionContext {
    return {
      runId: 'run_duplicate_test',
      scenario: 'duplicate_delivery',
      params: {
        eventType: 'payment.succeeded',
        eventId: 'evt_duplicate_test_1',
        orderId: overrides.orderId ?? 'ord_duplicate_test_1',
        deliveryCount: overrides.deliveryCount ?? 3,
        timeoutMs: overrides.timeoutMs ?? 2000,
        targetName: 'demo-receiver',
        targetUrl: process.env.DEMO_RECEIVER_URL ?? '',
        targetOracle: true,
      },
      report: {
        attempt: async (report) => {
          attempts.push(report);
        },
        assertion: async (report) => {
          assertions.push(report);
        },
      },
    };
  }

  const executor = (): DuplicateDeliveryExecutor =>
    new DuplicateDeliveryExecutor({} as unknown as ExecutorRegistry);

  beforeAll(async () => {
    receiver = createServer(handler);
    await new Promise<void>((resolve) => receiver.listen(0, '127.0.0.1', resolve));
    receiverUrl = `http://127.0.0.1:${(receiver.address() as AddressInfo).port}`;
  });

  afterAll(async () => {
    receiver.closeAllConnections();
    await new Promise<void>((resolve) => receiver.close(() => resolve()));
    delete process.env.DEMO_RECEIVER_URL;
  });

  beforeEach(() => {
    requests = [];
    attempts = [];
    assertions = [];
    behavior.status = 201;
    behavior.delayMs = 0;
    behavior.effectCount = 1;
    behavior.oracleStatus = 200;
    behavior.oracleDelayMs = 0;
    behavior.redirectTo = '';
    process.env.DEMO_RECEIVER_URL = receiverUrl;
  });

  it('passes when three same-event deliveries produce one business effect', async () => {
    const result = await executor().execute(makeContext());

    expect(result.status).toBe('passed');
    expect(result.summary).toContain('3 attempts');

    const deliveries = requests.filter((r) => r.method === 'POST');
    expect(deliveries).toHaveLength(3);
    expect(deliveries.map((r) => r.body?.eventId)).toEqual([
      'evt_duplicate_test_1',
      'evt_duplicate_test_1',
      'evt_duplicate_test_1',
    ]);
    expect(deliveries.every((r) => r.path === '/api/webhooks/demo')).toBe(true);

    expect(attempts.map((a) => a.attemptNumber)).toEqual([1, 2, 3]);
    expect(attempts.every((a) => a.httpStatus === 201 && a.outcome === 'delivered')).toBe(true);

    expect(assertions).toEqual([
      { name: ASSERTION_ATTEMPT_COUNT, expected: '3', actual: '3', passed: true },
      { name: ASSERTION_DELIVERIES_ACCEPTED, expected: '3', actual: '3', passed: true },
      { name: ASSERTION_EFFECT_ONCE, expected: '1', actual: '1', passed: true },
    ]);
  });

  it('honors a custom delivery count and still requires one business effect', async () => {
    const result = await executor().execute(makeContext({ deliveryCount: 5 }));

    expect(result.status).toBe('passed');
    expect(requests.filter((r) => r.method === 'POST')).toHaveLength(5);
    expect(attempts.map((a) => a.attemptNumber)).toEqual([1, 2, 3, 4, 5]);
    expect(assertions.find((a) => a.name === ASSERTION_ATTEMPT_COUNT)).toMatchObject({
      expected: '5',
      actual: '5',
      passed: true,
    });
  });

  it('fails when duplicate deliveries produce more than one business effect', async () => {
    behavior.effectCount = 3;
    const result = await executor().execute(makeContext());

    expect(result.status).toBe('failed');
    expect(result.summary).toContain(`${ASSERTION_EFFECT_ONCE} expected 1, got 3`);

    const effectAssertion = assertions.find((a) => a.name === ASSERTION_EFFECT_ONCE);
    expect(effectAssertion).toEqual({
      name: ASSERTION_EFFECT_ONCE,
      expected: '1',
      actual: '3',
      passed: false,
    });
    expect(assertions.find((a) => a.name === ASSERTION_ATTEMPT_COUNT)?.passed).toBe(true);
  });

  it('fails when deliveries are rejected by the receiver', async () => {
    behavior.status = 500;
    behavior.effectCount = 0;
    const result = await executor().execute(makeContext());

    expect(result.status).toBe('failed');
    expect(attempts.every((a) => a.outcome === 'error' && a.httpStatus === 500)).toBe(true);
    expect(assertions.find((a) => a.name === ASSERTION_DELIVERIES_ACCEPTED)).toEqual({
      name: ASSERTION_DELIVERIES_ACCEPTED,
      expected: '3',
      actual: '0',
      passed: false,
    });
  });

  it('records a bounded timeout as a timeout attempt', async () => {
    behavior.delayMs = 500;
    behavior.effectCount = 0;
    const result = await executor().execute(makeContext({ timeoutMs: 100 }));

    expect(result.status).toBe('failed');
    expect(attempts).toHaveLength(3);
    expect(attempts.every((a) => a.outcome === 'timeout')).toBe(true);
    expect(attempts[0]?.errorDetail).toContain('100 ms');
  });

  it('treats a missing order at the oracle as zero effects (assertion failure, not error)', async () => {
    behavior.status = 500;
    behavior.oracleStatus = 404;
    const result = await executor().execute(makeContext());

    expect(result.status).toBe('failed');
    expect(assertions.find((a) => a.name === ASSERTION_EFFECT_ONCE)).toMatchObject({
      actual: '0',
      passed: false,
    });
  });

  it('throws (execution error) when the oracle is unavailable or invalid', async () => {
    behavior.oracleStatus = 500;
    await expect(executor().execute(makeContext())).rejects.toThrow(
      'Receiver test oracle returned HTTP 500',
    );
  });

  it('resumes remaining deliveries from recorded attempts', async () => {
    const context = makeContext({ deliveryCount: 5 });
    context.resumeAttempts = [
      {
        attemptNumber: 1,
        eventId: 'evt_duplicate_test_1',
        outcome: 'delivered',
        httpStatus: 201,
        durationMs: 10,
        errorDetail: null,
      },
      {
        attemptNumber: 2,
        eventId: 'evt_duplicate_test_1',
        outcome: 'delivered',
        httpStatus: 201,
        durationMs: 11,
        errorDetail: null,
      },
    ];

    const result = await executor().execute(context);

    expect(result.status).toBe('passed');
    expect(requests.filter((r) => r.method === 'POST')).toHaveLength(3);
    expect(attempts.map((a) => a.attemptNumber)).toEqual([3, 4, 5]);
    expect(assertions.find((a) => a.name === ASSERTION_ATTEMPT_COUNT)).toMatchObject({
      expected: '5',
      actual: '5',
      passed: true,
    });
    expect(result.summary).toContain('resumed from 2');
  });

  it('delivers to an allowlisted remote target without a business-effect oracle', async () => {
    process.env.TARGETS_JSON = JSON.stringify([
      { name: 'mock-remote', url: receiverUrl, allowPrivate: true, oracle: false },
    ]);
    try {
      const context = makeContext();
      context.params.targetName = 'mock-remote';
      context.params.targetUrl = receiverUrl;
      context.params.targetOracle = false;

      const result = await executor().execute(context);

      expect(result.status).toBe('passed');
      expect(attempts).toHaveLength(3);
      expect(assertions.map((a) => a.name)).toEqual([
        ASSERTION_ATTEMPT_COUNT,
        ASSERTION_DELIVERIES_ACCEPTED,
      ]);
      expect(result.summary).toContain('no oracle');
    } finally {
      delete process.env.TARGETS_JSON;
    }
  });

  it('refuses unknown and mismatched targets', async () => {
    const unknown = makeContext();
    unknown.params.targetName = 'nope';
    await expect(executor().execute(unknown)).rejects.toThrow('Unknown or disabled target');

    const mismatch = makeContext();
    mismatch.params.targetUrl = 'http://127.0.0.1:5999';
    await expect(executor().execute(mismatch)).rejects.toThrow('does not match');
  });

  it('refuses private targets that are not explicitly allowed', async () => {
    process.env.TARGETS_JSON = JSON.stringify([
      { name: 'private-target', url: receiverUrl, oracle: false },
    ]);
    try {
      const context = makeContext();
      context.params.targetName = 'private-target';
      context.params.targetUrl = receiverUrl;
      await expect(executor().execute(context)).rejects.toThrow('Unknown or disabled target');
    } finally {
      delete process.env.TARGETS_JSON;
    }
  });

  it('refuses to follow redirects from the target', async () => {
    behavior.redirectTo = 'http://127.0.0.1:5999/steal';
    const result = await executor().execute(makeContext());

    expect(result.status).toBe('failed');
    expect(attempts.every((a) => a.outcome === 'error')).toBe(true);
  });

  it('reports an execution error when the configured target is unreachable', async () => {
    process.env.DEMO_RECEIVER_URL = 'https://unreachable.invalid';
    await expect(executor().execute(makeContext())).rejects.toThrow('oracle unavailable');
  });

  it('rejects jobs that try to smuggle a destination or any extra field', () => {
    const valid = {
      schemaVersion: 1,
      runId: 'run_1',
      scenario: 'duplicate_delivery',
      params: {
        eventType: 'payment.succeeded',
        eventId: 'evt_1',
        orderId: 'ord_1',
        deliveryCount: 3,
        timeoutMs: 5000,
        targetName: 'demo-receiver',
        targetUrl: 'http://127.0.0.1:3100',
        targetOracle: true,
      },
    };
    expect(parseRunJob(valid)).not.toBeNull();
    expect(
      parseRunJob({
        ...valid,
        params: { ...valid.params, destinationUrl: 'https://evil.example.com' },
      }),
    ).toBeNull();
    expect(parseRunJob({ ...valid, destinationUrl: 'https://evil.example.com' })).toBeNull();
    expect(parseRunJob({ ...valid, params: { ...valid.params, url: 'https://evil.example.com' } })).toBeNull();

    // Optional bounded fault parameters are accepted...
    expect(
      parseRunJob({ ...valid, params: { ...valid.params, httpStatus: 503, delayMs: 2000 } }),
    ).not.toBeNull();
    // ...but out-of-range values are rejected.
    expect(parseRunJob({ ...valid, params: { ...valid.params, httpStatus: 200 } })).toBeNull();
    expect(parseRunJob({ ...valid, params: { ...valid.params, delayMs: 60000 } })).toBeNull();
  });
});

describe('isPrivateHost (egress guard)', () => {
  const privates = [
    'localhost',
    'sub.localhost',
    'host.local',
    '127.0.0.1',
    '10.0.0.1',
    '172.16.0.1',
    '192.168.1.1',
    '169.254.169.254',
    '0.0.0.0',
    // IPv6 loopback / unspecified / link-local / unique-local (bracketed + bare).
    '[::1]',
    '::1',
    '[::]',
    '::',
    '[fe80::1]',
    'fe80::1',
    '[fc00::1]',
    'fc00::1',
    // IPv4-mapped and IPv4-compatible IPv6 smuggling a private IPv4.
    '[::ffff:127.0.0.1]',
    '::ffff:127.0.0.1',
    '[::ffff:10.0.0.1]',
    '::ffff:10.0.0.1',
    '[::ffff:169.254.169.254]',
    '::ffff:169.254.169.254',
    '[::127.0.0.1]',
    '::127.0.0.1',
  ];

  const publics = [
    'example.com',
    'api.github.com',
    '93.184.216.34',
    '1.1.1.1',
    '8.8.8.8',
    '[2606:4700:4700::1111]',
    '2606:4700:4700::1111',
    '[::ffff:8.8.8.8]',
    '::ffff:8.8.8.8',
  ];

  for (const host of privates) {
    it(`flags ${host} as private`, () => {
      expect(isPrivateHost(host)).toBe(true);
    });
  }
  for (const host of publics) {
    it(`does not flag ${host} as private`, () => {
      expect(isPrivateHost(host)).toBe(false);
    });
  }
});



