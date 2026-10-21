import { Injectable } from '@nestjs/common';
import { ScenarioExecutor } from './executor';

/** Scenario executors register themselves here; unknown scenarios fail explicitly. */
@Injectable()
export class ExecutorRegistry {
  private readonly executors = new Map<string, ScenarioExecutor>();

  register(executor: ScenarioExecutor): void {
    this.executors.set(executor.scenario, executor);
  }

  get(scenario: string): ScenarioExecutor | undefined {
    return this.executors.get(scenario);
  }
}
