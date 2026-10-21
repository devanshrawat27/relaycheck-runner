# RelayCheck Scenario Runner

NestJS worker. Consumes versioned run jobs from RabbitMQ, claims each run
exactly once through the Control API, executes the scenario, and reports
lifecycle/evidence through the Control API's authenticated internal
endpoints. It never connects to the PostgreSQL platform or demo databases.

Status: `duplicate_delivery` execution implemented and verified (2026-10-21).
The runner sends the same event ID three times to the fixed configured demo
receiver and records every attempt (number, HTTP status, duration, outcome).
The verdict (attempt-count + business-effect assertions) lands in the next
task; until then a completed delivery pass withholds its verdict and the run
finalizes as `error` with an explicit reason — the runner never fakes a pass.

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

See `.env.example`: `AMQP_URL`, `CONTROL_API_URL`, `INTERNAL_API_TOKEN`,
`DEMO_RECEIVER_URL`.

`DEMO_RECEIVER_URL` defaults to `http://localhost:3100` and is validated on
every execution: only `localhost`/`127.0.0.1` targets are accepted, so a
misconfiguration or any job payload can never turn the runner into an
arbitrary-URL sender. Job validation also rejects unknown fields, so a
destination cannot be smuggled in through the queue.

## duplicate_delivery scenario

- Sends the SAME `eventId` `deliveryCount` times (fixed to 3 by the Control
  API) to `POST /api/webhooks/demo` on the configured receiver.
- Records one attempt per delivery through the Control API: attempt number,
  HTTP status, duration, and outcome (`delivered` / `error` / `timeout`).
- Applies a bounded per-request timeout (`timeoutMs` from the validated job)
  using `AbortSignal.timeout`.

## Commands

- `npm install` / `npm ci` — install dependencies.
- `npm run lint` / `npm run type-check` — static checks.
- `npm test` — Jest e2e suite: real RabbitMQ consumption against a fake
  Control API, covering claim/execute/finalize, duplicate-job conflicts,
  assertion-failure vs execution-error, and malformed-job discarding.
- `npm run build` / `npm start` — build and run the worker.

Package manager: **npm** (lockfiles are committed). Node >= 20.
