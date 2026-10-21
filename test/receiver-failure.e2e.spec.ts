import { createServer, IncomingMessage, Server, ServerResponse } from 'http';
import { AddressInfo } from 'net';
import { ExecutorRegistry } from '../src/execution/executor-registry';
import { AssertionReport, AttemptReport, ExecutionContext } from '../src/execution/executor';
import {
  ASSERTION_NO_EFFECT,
  ASSERTION_RECEIVER_STATUS,
  ASSERTION_TIMED_OUT,
  ReceiverFaultExecutor,
  SCENARIO_RECEIVER_FAILURE,
  SCENARIO_RECEIVER_TIMEOUT,
} from '../src/scenarios/receiver-failure.executor';

interface FaultState {
  mode: 'error' | 'delay';
  httpStatus?: number;
  delayMs?: number;
}

describe('Controlled receiver failure/timeout scenarios (e2e)', () => {
  jest.setTimeout(30000);

  let receiver: Server;
  let receiverUrl: string;
  let faults: FaultState | null;
  let faultsAvailable: boolean;
  let ignoreFault: boolean;
  let attempts: AttemptReport[];
  let assertions: AssertionReport[];
  let faultWrites: number;
  let faultClears: number;

  function handler(req: IncomingMessage, res: ServerResponse): void {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString();
      res.setHeader('content-type', 'application/json');
      res.setHeader('connection', 'close');

      if (req.url === '/api/__test__/faults') {
        if (!faultsAvailable) {
          res.statusCode = 404;
          res.end(JSON.stringify({ message: 'not found' }));
          return;
        }
        if (req.method === 'POST') {
          faults = JSON.parse(raw) as FaultState;
          faultWrites += 1;
          res.statusCode = 201;
          res.end(JSON.stringify(faults));
          return;
        }
        if (req.method === 'DELETE') {
          faults = null;
          faultClears += 1;
          res.statusCode = 200;
          res.end(JSON.stringify({ mode: 'off' }));
          return;
        }
        res.statusCode = 200;
        res.end(JSON.stringify(faults ?? { mode: 'off' }));
        return;
      }

      if (req.method === 'GET' && req.url?.startsWith('/api/__test__/orders/')) {
        res.statusCode = 404;
        res.end(JSON.stringify({ message: 'no order' }));
        return;
      }

      if (req.method === 'POST' && req.url === '/api/webhooks/demo') {
        const respond = (): void => {
          if (!ignoreFault && faults?.mode === 'error') {
            res.statusCode = faults.httpStatus ?? 503;
          } else {
            res.statusCode = 201;
          }
          res.end(JSON.stringify({ accepted: true }));
        };
        if (!ignoreFault && faults?.mode === 'delay') {
          setTimeout(respond, faults.delayMs ?? 0);
        } else {
          respond();
        }
        return;
      }

      res.statusCode = 404;
      res.end('{}');
    });
  }

  function makeContext(scenario: string, timeoutMs: number): ExecutionContext {
    return {
      runId: 'run_receiver_fault_test',
      scenario,
      params: {
        eventType: 'payment.succeeded',
        eventId: 'evt_receiver_fault_1',
        orderId: 'ord_receiver_fault_1',
        deliveryCount: 1,
        timeoutMs,
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

  const executor = (): ReceiverFaultExecutor =>
    new ReceiverFaultExecutor({} as unknown as ExecutorRegistry);

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
    process.env.DEMO_RECEIVER_URL = receiverUrl;
    faults = null;
    faultsAvailable = true;
    ignoreFault = false;
    attempts = [];
    assertions = [];
    faultWrites = 0;
    faultClears = 0;
  });

  it('receiver_failure passes when the receiver returns the expected 503 and applies no effect', async () => {
    const result = await executor().execute(makeContext(SCENARIO_RECEIVER_FAILURE, 3000));

    expect(result.status).toBe('passed');
    expect(attempts).toHaveLength(1);
    expect(attempts[0]).toMatchObject({ httpStatus: 503, outcome: 'error', attemptNumber: 1 });

    expect(assertions.find((a) => a.name === ASSERTION_RECEIVER_STATUS)).toEqual({
      name: ASSERTION_RECEIVER_STATUS,
      expected: '503',
      actual: '503',
      passed: true,
    });
    expect(assertions.find((a) => a.name === ASSERTION_NO_EFFECT)).toEqual({
      name: ASSERTION_NO_EFFECT,
      expected: '0',
      actual: '0',
      passed: true,
    });

    // The fault must be cleared afterwards.
    expect(faultWrites).toBe(1);
    expect(faultClears).toBe(1);
    expect(faults).toBeNull();
  });

  it('receiver_failure fails when the receiver does not return the expected error', async () => {
    ignoreFault = true;
    const result = await executor().execute(makeContext(SCENARIO_RECEIVER_FAILURE, 3000));

    expect(result.status).toBe('failed');
    expect(result.summary).toContain(`${ASSERTION_RECEIVER_STATUS} expected 503, got 201`);
    expect(assertions.find((a) => a.name === ASSERTION_RECEIVER_STATUS)?.passed).toBe(false);
  });

  it('receiver_timeout passes when the receiver responds slower than the client timeout', async () => {
    const result = await executor().execute(makeContext(SCENARIO_RECEIVER_TIMEOUT, 100));

    expect(result.status).toBe('passed');
    expect(attempts).toHaveLength(1);
    expect(attempts[0]?.outcome).toBe('timeout');
    expect(assertions.find((a) => a.name === ASSERTION_TIMED_OUT)).toEqual({
      name: ASSERTION_TIMED_OUT,
      expected: 'timeout',
      actual: 'timeout',
      passed: true,
    });
    expect(faults).toBeNull();
  });

  it('fails with an execution error when fault controls are unavailable (test mode off)', async () => {
    faultsAvailable = false;
    await expect(
      executor().execute(makeContext(SCENARIO_RECEIVER_FAILURE, 3000)),
    ).rejects.toThrow('RELAYCHECK_TEST_MODE is not enabled');
    expect(attempts).toHaveLength(0);
  });
});
