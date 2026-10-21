import { Injectable, Logger } from '@nestjs/common';
import { connect } from 'amqplib';
import type { Channel, ChannelModel, ConsumeMessage } from 'amqplib';
import { LifecycleService, ProcessOutcome } from '../execution/lifecycle.service';
import { parseRunJob } from './run-job';

export const RUN_JOBS_QUEUE = 'relaycheck.run.jobs';
/** Dead-letter destination for jobs that cannot be processed (poison or exhausted retries). */
export const RUN_JOBS_DLQ = 'relaycheck.run.jobs.dlq';

const MAX_REDELIVERIES = 5;
const RETRY_HEADER = 'x-relaycheck-retry';

/**
 * Consumes versioned run jobs and owns the ack policy:
 *
 *  - ack        → processed successfully, or safely ignored (duplicate/finished run)
 *  - retry      → republish with an incremented retry header (bounded)
 *  - dead-letter→ publish to the DLQ, then ack (never lost silently)
 *  - malformed  → DLQ (no requeue loops)
 *
 * Retries are republished rather than nack-requeued so the count is bounded
 * and observable on the message itself.
 */
@Injectable()
export class RunJobConsumer {
  private readonly logger = new Logger(RunJobConsumer.name);
  private connection?: ChannelModel;
  private channel?: Channel;

  constructor(private readonly lifecycle: LifecycleService) {}

  async start(): Promise<void> {
    const url = process.env.AMQP_URL ?? 'amqp://guest:guest@localhost:5672';
    this.connection = await connect(url);
    this.channel = await this.connection.createChannel();
    await this.channel.assertQueue(RUN_JOBS_QUEUE, { durable: true });
    await this.channel.assertQueue(RUN_JOBS_DLQ, { durable: true });
    await this.channel.prefetch(1);
    await this.channel.consume(RUN_JOBS_QUEUE, (message) => {
      void this.handleMessage(message).catch((error: unknown) => {
        this.logger.error(`Unhandled consumer failure: ${error instanceof Error ? error.message : error}`);
      });
    });
    this.logger.log(`Consuming ${RUN_JOBS_QUEUE} (dead-letter: ${RUN_JOBS_DLQ})`);
  }

  private async handleMessage(message: ConsumeMessage | null): Promise<void> {
    if (!message || !this.channel) {
      return;
    }

    let raw: unknown;
    try {
      raw = JSON.parse(message.content.toString());
    } catch {
      await this.deadLetter(message, 'Malformed job: invalid JSON');
      return;
    }

    const job = parseRunJob(raw);
    if (!job) {
      await this.deadLetter(message, 'Malformed job: schema validation failed');
      return;
    }

    const outcome = await this.lifecycle.process(job);
    await this.applyOutcome(message, outcome);
  }

  private async applyOutcome(message: ConsumeMessage, outcome: ProcessOutcome): Promise<void> {
    const channel = this.channel;
    if (!channel) {
      return;
    }

    if (outcome.action === 'ack') {
      channel.ack(message);
      return;
    }

    if (outcome.action === 'dead-letter') {
      await this.deadLetter(message, outcome.reason);
      return;
    }

    const attempt = this.retryCount(message);
    if (attempt >= MAX_REDELIVERIES) {
      await this.deadLetter(message, `Retries exhausted (${attempt}): ${outcome.reason}`);
      return;
    }

    this.logger.warn(
      `Republishing job (retry ${attempt + 1}/${MAX_REDELIVERIES}): ${outcome.reason}`,
    );
    channel.sendToQueue(RUN_JOBS_QUEUE, message.content, {
      persistent: true,
      contentType: message.properties.contentType ?? 'application/json',
      headers: { ...(message.properties.headers ?? {}), [RETRY_HEADER]: attempt + 1 },
    });
    channel.ack(message);
  }

  private async deadLetter(message: ConsumeMessage, reason: string): Promise<void> {
    const channel = this.channel;
    if (!channel) {
      return;
    }
    this.logger.error(`Dead-lettering job: ${reason}`);
    channel.sendToQueue(RUN_JOBS_DLQ, message.content, {
      persistent: true,
      contentType: message.properties.contentType ?? 'application/json',
      headers: { ...(message.properties.headers ?? {}), 'x-relaycheck-error': reason.slice(0, 500) },
    });
    channel.ack(message);
  }

  private retryCount(message: ConsumeMessage): number {
    const raw = message.properties.headers?.[RETRY_HEADER];
    return typeof raw === 'number' ? raw : 0;
  }

  async stop(): Promise<void> {
    await this.channel?.close();
    await this.connection?.close();
  }
}
