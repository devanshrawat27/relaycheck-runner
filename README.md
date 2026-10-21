# RelayCheck Scenario Runner

NestJS worker. Consumes versioned run jobs from RabbitMQ, claims each run
exactly once through the Control API, executes the scenario, and reports
lifecycle/evidence through the Control API's authenticated internal
endpoints. It never connects to the PostgreSQL platform or demo databases.

Status: lifecycle engine implemented and verified (2026-10-20). The
`duplicate_delivery` execution logic lands in the next task; a scenario
without a registered executor finalizes as `error` — the runner fails
loudly and never fakes a pass.

## Lifecycle

1. Consume `relaycheck.run.jobs` (durable queue, prefetch 1).
2. Validate the job at the worker boundary (`schemaVersion: 1`, run ID,
   scenario, bounded params). Malformed messages are discarded without
   requeue — no poison-message loops.
3. Claim via `POST /api/internal/runs/:id/claim`: `queued → running` happens
   exactly once. HTTP 409 means another worker already owns the run, so the
   duplicate job is acknowledged and ignored.
4. Run the scenario executor, reporting attempts/assertions through the
   Control API.
5. Finalize: `passed`/`failed` for assertion outcomes, `error` when
   execution itself could not complete reliably.

## Configuration

See `.env.example`: `AMQP_URL`, `CONTROL_API_URL`, `INTERNAL_API_TOKEN`.
The fixed demo receiver target is server-configured; no job or user input can
change it.

## Commands

- `npm install` / `npm ci` — install dependencies.
- `npm run lint` / `npm run type-check` — static checks.
- `npm test` — Jest e2e suite: real RabbitMQ consumption against a fake
  Control API, covering claim/execute/finalize, duplicate-job conflicts,
  assertion-failure vs execution-error, and malformed-job discarding.
- `npm run build` / `npm start` — build and run the worker.

Package manager: **npm** (lockfiles are committed). Node >= 20.
