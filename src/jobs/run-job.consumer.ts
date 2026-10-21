import { Injectable, Logger } from '@nestjs/common';
import { connect } from 'amqplib';
import type { Channel, ChannelModel, ConsumeMessage } from 'amqplib';
import { LifecycleService } from '../execution/lifecycle.service';
import { parseRunJob } from './run-job';

export const RUN_JOBS_QUEUE = 'relaycheck.run.jobs';

/**
 * Consumes versioned run jobs. Malformed messages are discarded (no requeue
 * loops); valid jobs are handed to the lifecycle service and acknowledged
 * only after processing returns.
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
    await this.channel.prefetch(1);
    await this.channel.consume(RUN_JOBS_QUEUE, (message) => {
      void this.handleMessage(message).catch((error: unknown) => {
        this.logger.error(`Unhandled consumer failure: ${error instanceof Error ? error.message : error}`);
      });
    });
    this.logger.log(`Consuming ${RUN_JOBS_QUEUE}`);
  }

  private async handleMessage(message: ConsumeMessage | null): Promise<void> {
    if (!message || !this.channel) {
      return;
    }
    const channel = this.channel;

    let raw: unknown;
    try {
      raw = JSON.parse(message.content.toString());
    } catch {
      this.logger.warn('Discarding malformed job: invalid JSON');
      channel.nack(message, false, false);
      return;
    }

    const job = parseRunJob(raw);
    if (!job) {
      this.logger.warn('Discarding malformed job: schema validation failed');
      channel.nack(message, false, false);
      return;
    }

    await this.lifecycle.process(job);
    channel.ack(message);
  }

  async stop(): Promise<void> {
    await this.channel?.close();
    await this.connection?.close();
  }
}
