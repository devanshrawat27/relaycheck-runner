import { createServer, IncomingMessage, Server, ServerResponse } from 'http';
import { AddressInfo } from 'net';
import { ExecutorRegistry } from '../src/execution/executor-registry';
import { AttemptReport, ExecutionContext } from '../src/execution/executor';
import { DuplicateDeliveryExecutor } from '../src/scenarios/duplicate-delivery.executor';
import { parseRunJob } from '../src/jobs/run-job';

interface RecordedRequest {
  path: string;
  body: Record<string, unknown>;
}

describe('duplicate_delivery executor (e2e)', () => {
  jest.setTimeout(30000);

  let receiver: Server;
  let receiverUrl: string;
  let requests: RecordedRequest[];
  let attempts: AttemptReport[];
  const behavior = { status: 201, delayMs: 0 };

  function handler(req: IncomingMessage, res: ServerResponse): void {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => {
      requests.push({
        path: req.url ?? '',
        body: JSON.parse(Buffer.concat(chunks).toString()) as Record<string, unknown>,
      });
      res.setHeader('content-type', 'application/json');
      res.setHeader('connection', 'close');
      const respond = (): void => {
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

  function makeContext(overrides: { timeoutMs?: number; deliveryCount?: number } = {}): ExecutionContext {
    return {
      runId: 'run_duplicate_test',
      scenario: 'duplicate_delivery',
      params: {
        eventType: 'payment.succeeded',
        eventId: 'evt_duplicate_test_1',
        orderId: 'ord_duplicate_test_1',
        deliveryCount: overrides.deliveryCount ?? 3,
        timeoutMs: overrides.timeoutMs ?? 2000,
      },
      report: {
        attempt: async (report) => {
          attempts.push(report);
        },
        assertion: async () => undefined,
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
    behavior.status = 201;
    behavior.delayMs = 0;
    process.env.DEMO_RECEIVER_URL = receiverUrl;
  });

  it('sends the same event ID three times and records every attempt', async () => {
    await expect(executor().execute(makeContext())).rejects.toThrow(
      'Assertion evaluation is not implemented yet',
    );

    expect(requests).toHaveLength(3);
    const eventIds = requests.map((r) => r.body.eventId);
    expect(eventIds).toEqual(['evt_duplicate_test_1', 'evt_duplicate_test_1', 'evt_duplicate_test_1']);
    expect(requests.every((r) => r.path === '/api/webhooks/demo')).toBe(true);
    expect(requests[0]?.body.orderId).toBe('ord_duplicate_test_1');
    // nothing resembling a destination is ever part of the payload
    expect(requests.every((r) => !('destinationUrl' in r.body) && !('url' in r.body))).toBe(true);

    expect(attempts).toHaveLength(3);
    expect(attempts.map((a) => a.attemptNumber)).toEqual([1, 2, 3]);
    expect(attempts.every((a) => a.eventId === 'evt_duplicate_test_1')).toBe(true);
    expect(attempts.every((a) => a.httpStatus === 201 && a.outcome === 'delivered')).toBe(true);
    expect(attempts.every((a) => typeof a.durationMs === 'number' && a.durationMs >= 0)).toBe(true);
  });

  it('records non-2xx responses as error attempts with their status', async () => {
    behavior.status = 500;
    await expect(executor().execute(makeContext())).rejects.toThrow();

    expect(attempts).toHaveLength(3);
    expect(attempts.every((a) => a.outcome === 'error' && a.httpStatus === 500)).toBe(true);
    expect(attempts[0]?.errorDetail).toContain('HTTP 500');
  });

  it('records a bounded timeout as a timeout attempt', async () => {
    behavior.delayMs = 500;
    await expect(executor().execute(makeContext({ timeoutMs: 100 }))).rejects.toThrow();

    expect(attempts).toHaveLength(3);
    expect(attempts.every((a) => a.outcome === 'timeout')).toBe(true);
    expect(attempts.every((a) => a.httpStatus === undefined)).toBe(true);
    expect(attempts[0]?.errorDetail).toContain('100 ms');
  });

  it('refuses to deliver to a non-local configured target', async () => {
    process.env.DEMO_RECEIVER_URL = 'https://evil.example.com';
    await expect(executor().execute(makeContext())).rejects.toThrow('must be local');
    expect(requests).toHaveLength(0);
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
  });
});
