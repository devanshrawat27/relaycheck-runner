# RelayCheck Scenario Runner

NestJS worker. Consumes versioned run jobs from RabbitMQ, claims each run
exactly once through the Control API, executes the scenario, and reports
lifecycle/evidence through the Control API's authenticated internal
endpoints. It never connects to the PostgreSQL platform or demo databases.

Status: three scenarios implemented and verified (2026-10-21):
`duplicate_delivery`, `receiver_failure`, and `receiver_timeout`. Job
processing is resilient: transient Control API failures are retried with a
bounded count, permanent failures and poisoned messages go to a dead-letter
queue, and a duplicate job for a still-running run recovers it to `error`
rather than leaving it stuck.

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

## Failure handling

- Claims use a **lease** (`workerId` + expiry). While executing, the runner
  heartbeats (default every 10 s, override `RUNNER_HEARTBEAT_MS`). A duplicate
  job may recover a run **only if the lease has expired**; an actively
  executing worker is never prematurely failed.
- Transient Control API problems (network/5xx) are retried in-process
  (bounded) and, if still failing, the job message is republished with a
  retry header (max 5) before being dead-lettered.
- Republished/retry/dead-letter copies are sent over a **confirm channel**;
  the original message is acknowledged only after the broker confirms the
  copy. Unconfirmed publishes leave the original for redelivery.
- Permanent problems (401/403/404) are dead-lettered immediately.
- Malformed jobs go to the dead-letter queue — never a requeue loop.
- The consumer reconnects with exponential backoff (max 30 s) after a channel
  or connection failure.

Queues: `relaycheck.run.jobs` (work) and `relaycheck.run.jobs.dlq`
(poison/exhausted).

## Dead-letter tooling

From this repository:

```bash
npm run dlq                 # list dead-lettered messages (side-effect free)
npm run dlq -- --limit 100  # list more
npm run dlq -- --replay     # republish to the work queue, then ack
npm run dlq -- --purge      # delete all dead-lettered messages
```

Each listing shows the run ID, retry count, and the recorded error reason.

## Scenarios

| Scenario | Behaviour |
|---|---|
| `duplicate_delivery` | 3 deliveries of one event ID; expects 3 attempts and exactly 1 business effect |
| `receiver_failure` | injects a test-only 503; expects that status and **no** business effect |
| `receiver_timeout` | injects a delay beyond the client timeout; expects a recorded timeout attempt |

Failure/timeout scenarios require the receiver to run with
`RELAYCHECK_TEST_MODE=true`; otherwise the run ends as `error` (fail closed).
The executor clears any injected fault afterwards.

## duplicate_delivery scenario

- Sends the SAME `eventId` `deliveryCount` times (fixed to 3 by the Control
  API) to `POST /api/webhooks/demo` on the configured receiver.
- Records one attempt per delivery through the Control API: attempt number,
  HTTP status, duration, and outcome (`delivered` / `error` / `timeout`).
- Applies a bounded per-request timeout (`timeoutMs` from the validated job)
  using `AbortSignal.timeout`.
- Evaluates and persists three assertions with expected and actual values:

  | Assertion | Expected | Meaning |
  |---|---|---|
  | `delivery_attempt_count` | 3 | every delivery was attempted and recorded |
  | `deliveries_accepted` | 3 | every delivery was answered with 2xx |
  | `business_effect_applied_once` | 1 | receiver oracle reports exactly one effect |

- Verdict rules: any failed assertion → run `failed`; oracle unreachable or
  invalid → run `error`. A missing order at the oracle counts as **0**
  effects, which is an assertion failure (the execution itself completed).

## Commands

- `npm install` / `npm ci` — install dependencies.
- `npm run lint` / `npm run type-check` — static checks.
- `npm test` — Jest e2e suite: real RabbitMQ consumption against a fake
  Control API, covering claim/execute/finalize, duplicate-job conflicts,
  assertion-failure vs execution-error, and malformed-job discarding.
- `npm run build` / `npm start` — build and run the worker.

Package manager: **npm** (lockfiles are committed). Node >= 20.
