#!/usr/bin/env node
/**
 * Dead-letter queue inspection and recovery for the run-job queue.
 *
 * Usage (from relaycheck-runner):
 *   npm run dlq                 # list messages (default 20, kept in the queue)
 *   npm run dlq -- --limit 100  # list more
 *   npm run dlq -- --replay     # republish to the work queue, then ack
 *   npm run dlq -- --purge      # delete all dead-lettered messages
 *
 * Environment: AMQP_URL (default amqp://guest:guest@localhost:5672)
 */

import { connect } from 'amqplib';

const AMQP_URL = process.env.AMQP_URL ?? 'amqp://guest:guest@localhost:5672';
const WORK_QUEUE = 'relaycheck.run.jobs';
const DLQ = 'relaycheck.run.jobs.dlq';

const args = process.argv.slice(2);
const replay = args.includes('--replay');
const purge = args.includes('--purge');
const limitIndex = args.indexOf('--limit');
const limit = limitIndex >= 0 ? Number(args[limitIndex + 1] ?? 20) : 20;

function describe(message) {
  let runId = 'unknown';
  try {
    const parsed = JSON.parse(message.content.toString());
    runId = typeof parsed.runId === 'string' ? parsed.runId : 'unknown';
  } catch {
    runId = 'invalid-json';
  }
  const errorHeader = message.properties.headers?.['x-relaycheck-error'];
  const retries = message.properties.headers?.['x-relaycheck-retry'];
  return { runId, retries: retries ?? 0, reason: String(errorHeader ?? 'unknown') };
}

async function main() {
  const connection = await connect(AMQP_URL);
  const channel = await connection.createChannel();
  try {
    await channel.assertQueue(WORK_QUEUE, { durable: true });
    await channel.assertQueue(DLQ, { durable: true });

    if (purge) {
      let purged = 0;
      for (;;) {
        const message = await channel.get(DLQ, { noAck: true });
        if (!message) {
          break;
        }
        purged += 1;
      }
      console.log(`[dlq] purged ${purged} message(s)`);
      return;
    }

    // Collect first (messages stay unacked, so they cannot be re-fetched),
    // print, then replay or requeue.
    const collected = [];
    while (collected.length < limit) {
      const message = await channel.get(DLQ, { noAck: false });
      if (!message) {
        break;
      }
      collected.push(message);
    }

    let replayed = 0;
    for (const [index, message] of collected.entries()) {
      const info = describe(message);
      console.log(
        `[dlq] ${index + 1}. runId=${info.runId} retries=${info.retries} reason=${info.reason}`,
      );

      if (replay) {
        channel.sendToQueue(WORK_QUEUE, message.content, {
          persistent: true,
          contentType: message.properties.contentType ?? 'application/json',
          headers: message.properties.headers ?? {},
        });
        await channel.waitForConfirms();
        channel.ack(message);
        replayed += 1;
      } else {
        channel.nack(message, false, true);
      }
    }

    // nack() is asynchronous; give the broker a moment to requeue.
    await new Promise((resolve) => setTimeout(resolve, 300));
    const remaining = await channel.checkQueue(DLQ);
    console.log(
      `[dlq] listed ${collected.length} message(s); replayed ${replayed}; still queued: ${remaining.messageCount}`,
    );
  } finally {
    await channel.close();
    await connection.close();
  }
}

main().catch((error) => {
  console.error(`[dlq] ERROR: ${error instanceof Error ? error.message : error}`);
  process.exitCode = 1;
});
