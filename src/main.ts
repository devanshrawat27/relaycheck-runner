import 'reflect-metadata';
import 'dotenv/config';
import { createServer } from 'node:http';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { RunJobConsumer } from './jobs/run-job.consumer';

async function bootstrap(): Promise<void> {
  const logger = new Logger('Bootstrap');
  const app = await NestFactory.createApplicationContext(AppModule);
  const consumer = app.get(RunJobConsumer);

  await consumer.start();
  logger.log('Scenario runner is consuming jobs');

  // Optional HTTP health server so the runner can also run as a Render *web
  // service* (Render has no free background-worker plan). It is inert locally
  // because PORT is unset there. A self-ping keeps a free instance awake.
  const port = Number(process.env.PORT ?? 0);
  if (port > 0) {
    const server = createServer((_req, res) => {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', service: 'scenario-runner' }));
    });
    server.listen(port, '0.0.0.0', () => {
      logger.log(`Runner health server listening on 0.0.0.0:${port}`);
    });

    const selfUrl = process.env.SELF_URL;
    if (selfUrl) {
      const interval = setInterval(() => {
        void fetch(`${selfUrl.replace(/\/$/, '')}/`).catch(() => undefined);
      }, 10 * 60 * 1000);
      interval.unref();
    }
  }

  const shutdown = async (): Promise<void> => {
    logger.log('Shutting down scenario runner');
    await consumer.stop();
    await app.close();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown());
  process.on('SIGTERM', () => void shutdown());
}

void bootstrap();
