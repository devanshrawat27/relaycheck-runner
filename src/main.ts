import 'reflect-metadata';
import 'dotenv/config';
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
