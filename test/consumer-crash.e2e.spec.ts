import 'dotenv/config';
import { INestApplicationContext } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { connect } from 'amqplib';
import type { ChannelModel } from 'amqplib';
import { AppModule } from '../src/app.module';
import { LifecycleService } from '../src/execution/lifecycle.service';
import { RUN_JOBS_DLQ, RUN_JOBS_QUEUE, RunJobConsumer } from '../src/jobs/run-job.consumer';

const AMQP = 'amqp://guest:guest@localhost:5672';

/**
 * Regression guard: if lifecycle.process() throws unexpectedly, the consumer
 * must not leave the message unacknowledged (which would stall prefetch 1).
 * It applies the bounded retry policy and eventually dead-letters the job.
 */
describe('Consumer crash safety (e2e)', () => {
  jest.setTimeout(90000);

  let app: INestApplicationContext;
  let consumer: RunJobConsumer;
  let pubConn: ChannelModel;

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

  async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 60000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (await predicate()) {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    throw new Error('Timed out waiting for condition');
  }

  beforeAll(async () => {
    process.env.AMQP_URL = AMQP;
    pubConn = await connect(AMQP);
    await drain(RUN_JOBS_QUEUE);
    await drain(RUN_JOBS_DLQ);

    const moduleRef = await Test.createTestingModule({ imports: [AppModule] })
      .overrideProvider(LifecycleService)
      .useValue({
        process: async (): Promise<never> => {
          throw new Error('unexpected lifecycle crash');
        },
      })
      .compile();
    await moduleRef.init();
    app = moduleRef;
    consumer = app.get(RunJobConsumer);
    await consumer.start();
  });

  afterAll(async () => {
    await consumer?.stop();
    await app?.close();
    await drain(RUN_JOBS_QUEUE);
    await drain(RUN_JOBS_DLQ);
    await pubConn?.close();
  });

  it('retries an unexpectedly failing job and dead-letters it instead of stalling', async () => {
    const channel = await pubConn.createChannel();
    try {
      await channel.assertQueue(RUN_JOBS_QUEUE, { durable: true });
      channel.sendToQueue(
        RUN_JOBS_QUEUE,
        Buffer.from(
          JSON.stringify({
            schemaVersion: 1,
            runId: 'run_crash_test',
            scenario: 'duplicate_delivery',
            params: {
              eventType: 'payment.succeeded',
              eventId: 'evt_crash_test',
              orderId: 'ord_crash_test',
              deliveryCount: 3,
              timeoutMs: 5000,
            },
          }),
        ),
        { persistent: true },
      );
    } finally {
      await channel.close();
    }

    await waitFor(async () => (await queueDepth(RUN_JOBS_DLQ)) >= 1);
    await waitFor(async () => (await queueDepth(RUN_JOBS_QUEUE)) === 0);
  });
});
