# RelayCheck Scenario Runner

NestJS worker. Consumes versioned run jobs from RabbitMQ, executes bounded
scenario deliveries against the configured local demo receiver, records
attempt evidence, evaluates assertions, and finalizes runs.

Ownership boundaries (see `ARCHITECTURE.md` at the workspace root):

- Owns execution logic only: bounded delivery, timing, assertions, and
  duplicate-job protection.
- Submits lifecycle updates, attempt records, and assertion results through
  private authenticated Control API endpoints.
- Does not connect to the platform database and never touches Demo Receiver
  tables; it uses the receiver's HTTP webhook and test-oracle endpoints only.
- Deliveries target the fixed configured local demo receiver only.

Status: workspace initialized (T001). Implementation starts at T030.
Tasks and acceptance criteria live in `TASKS.md` at the workspace root.

## Commands

- `npm install` - install dependencies (verified from a clean checkout).

`npm run type-check`, `npm run build`, `npm run test`, and `npm run lint` are
added when their service's phase task lands (see `TASKS.md`). No placeholder
scripts are included, so nothing here fails silently.

Package manager: **npm** (lockfiles are committed). Node >= 20.