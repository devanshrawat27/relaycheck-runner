import { Injectable, Logger } from '@nestjs/common';
import { connect } from 'amqplib';
import type { ChannelModel, ConfirmChannel, ConsumeMessage } from 'amqplib';
import { LifecycleService, ProcessOutcome } from '../execution/lifecycle.service';
import { parseRunJob } from './run-job';

export const RUN_JOBS_QUEUE = 'relaycheck.run.jobs';
/** Dead-letter destination for jobs that cannot be processed (poison or exhausted retries). */
export const RUN_JOBS_DLQ = 'relaycheck.run.jobs.dlq';

const MAX_REDELIVERIES = 5;
const RETRY_HEADER = 'x-relaycheck-retry';
const MAX_RECONNECT_DELAY_MS = 30000;

/**
 * Consumes versioned run jobs and owns the ack policy:
 *
 *  - ack        → processed successfully, or safely ignored (duplicate/finished run)
 *  - retry      → republish with an incremented retry header (bounded)
 *  - dead-letter→ publish to the DLQ, then ack (never lost silently)
 *  - malformed  → DLQ (no requeue loops)
 *
 * Republished copies are sent over a **confirm channel**: the original
 * message is acknowledged only after the broker confirms the copy. If the
 * publish cannot be confirmed, the original is requeued (or left unacked on
 * a closed channel, which the broker requeues) — nothing is silently lost.
 */
@Injectable()
export class RunJobConsumer {
  private readonly logger = new Logger(RunJobConsumer.name);
  private connection?: ChannelModel;
  private channel?: ConfirmChannel;
  private stopping = false;
  private connecting = false;
  private reconnectTimer?: NodeJS.Timeout;
  private reconnectAttempts = 0;

  constructor(private readonly lifecycle: LifecycleService) {}

  async start(): Promise<void> {
    this.stopping = false;
    await this.connectAndConsume();
  }

  private async connectAndConsume(): Promise<void> {
    if (this.connecting || this.stopping) {
      return;
    }
    this.connecting = true;
    try {
      const url = process.env.AMQP_URL ?? 'amqp://guest:guest@localhost:5672';
      const connection = await connect(url);
      this.connection = connection;
      connection.on('close', () => this.scheduleReconnect('connection closed'));
      connection.on('error', (error: Error) =>
        this.logger.warn(`RabbitMQ connection error: ${error.message}`),
      );

      const channel = await connection.createConfirmChannel();
      this.channel = channel;
      channel.on('close', () => this.scheduleReconnect('channel closed'));
      channel.on('error', (error: Error) =>
        this.logger.warn(`RabbitMQ channel error: ${error.message}`),
      );

      await channel.assertQueue(RUN_JOBS_QUEUE, { durable: true });
      await channel.assertQueue(RUN_JOBS_DLQ, { durable: true });
      await channel.prefetch(1);
      await channel.consume(RUN_JOBS_QUEUE, (message) => {
        void this.handleMessage(message).catch((error: unknown) => {
          this.logger.error(
            `Unhandled consumer failure: ${error instanceof Error ? error.message : error}`,
          );
        });
      });

      this.reconnectAttempts = 0;
      this.logger.log(`Consuming ${RUN_JOBS_QUEUE} (dead-letter: ${RUN_JOBS_DLQ})`);
    } catch (error) {
      this.logger.error(
        `Could not connect to RabbitMQ: ${error instanceof Error ? error.message : error}`,
      );
      this.scheduleReconnect('connect failed');
    } finally {
      this.connecting = false;
    }
  }

  private scheduleReconnect(reason: string): void {
    this.channel = undefined;
    if (this.stopping || this.reconnectTimer) {
      return;
    }
    const delay = Math.min(MAX_RECONNECT_DELAY_MS, 1000 * 2 ** this.reconnectAttempts);
    this.reconnectAttempts += 1;
    this.logger.warn(`Reconnecting in ${delay}ms (${reason})`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.connectAndConsume();
    }, delay);
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
    try {
      await this.publishConfirmed(RUN_JOBS_QUEUE, message, {
        ...(message.properties.headers ?? {}),
        [RETRY_HEADER]: attempt + 1,
      });
      channel.ack(message);
    } catch (error) {
      this.logger.error(
        `Retry republish was not confirmed; leaving original for redelivery: ${
          error instanceof Error ? error.message : error
        }`,
      );
      this.requeueOriginal(message);
    }
  }

  private async deadLetter(message: ConsumeMessage, reason: string): Promise<void> {
    const channel = this.channel;
    if (!channel) {
      return;
    }
    this.logger.error(`Dead-lettering job: ${reason}`);
    try {
      await this.publishConfirmed(RUN_JOBS_DLQ, message, {
        ...(message.properties.headers ?? {}),
        'x-relaycheck-error': reason.slice(0, 500),
      });
      channel.ack(message);
    } catch (error) {
      this.logger.error(
        `Dead-letter publish was not confirmed; leaving original for redelivery: ${
          error instanceof Error ? error.message : error
        }`,
      );
      this.requeueOriginal(message);
    }
  }

  /** Publish a copy and wait for broker confirmation before acknowledging the original. */
  private async publishConfirmed(
    queue: string,
    message: ConsumeMessage,
    headers: Record<string, unknown>,
  ): Promise<void> {
    const channel = this.channel;
    if (!channel) {
      throw new Error('No active channel');
    }
    channel.sendToQueue(queue, message.content, {
      persistent: true,
      contentType: message.properties.contentType ?? 'application/json',
      headers,
    });
    await channel.waitForConfirms();
  }

  private requeueOriginal(message: ConsumeMessage): void {
    try {
      this.channel?.nack(message, false, true);
    } catch {
      // Channel is closed; the broker requeues unacked messages on close and
      // the reconnect policy re-establishes consumption.
    }
  }

  private retryCount(message: ConsumeMessage): number {
    const raw = message.properties.headers?.[RETRY_HEADER];
    return typeof raw === 'number' ? raw : 0;
  }

  async stop(): Promise<void> {
    this.stopping = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    try {
      await this.channel?.close();
    } catch {
      // Already closed.
    }
    try {
      await this.connection?.close();
    } catch {
      // Already closed.
    }
  }
}
