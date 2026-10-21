import { Module } from '@nestjs/common';
import { ControlApiClient } from './control-api/control-api.client';
import { ExecutorRegistry } from './execution/executor-registry';
import { LifecycleService } from './execution/lifecycle.service';
import { RunJobConsumer } from './jobs/run-job.consumer';
import { DuplicateDeliveryExecutor } from './scenarios/duplicate-delivery.executor';

@Module({
  providers: [
    ControlApiClient,
    ExecutorRegistry,
    LifecycleService,
    RunJobConsumer,
    DuplicateDeliveryExecutor,
  ],
  exports: [RunJobConsumer, ExecutorRegistry],
})
export class AppModule {}
