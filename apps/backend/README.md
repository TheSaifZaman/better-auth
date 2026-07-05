# backend

NestJS API for the [better-auth monorepo](../../README.md). It is the **authentication server** and the **event producer** that feeds both broker pipelines.

## Responsibilities

- **Auth** — [better-auth](https://better-auth.com) (email/password, email verification, 2FA/TOTP, rate limiting, HaveIBeenPwned checks) mounted at `/api/auth`, wired through Drizzle/Postgres.
- **Run domain** — a minimal `workflow`/`run` state machine (`queued → running → completed`). `POST /runs` creates a run; `POST /runs/:id/advance` steps it. Both are `@Public`.
- **Dual-publish** — `EventBusService` fans every event to **both** brokers, fire-and-forget (a down broker logs, never fails the request):
  - **RabbitMQ** via a Nest `ClientProxy` → topic exchange `events`, routing key = event name (Nest envelope, consumed by `apps/notifier`).
  - **Kafka** via a **raw kafkajs `Producer`** → topic `run-events`, value = plain JSON, key = `workflowId` (falling back to `userId`). Plain JSON is the contract `apps/audit-go` parses.
- **Auth as a second producer** — an `@DatabaseHook` provider (`AuthEventsHook`) publishes `auth.user.created` / `auth.session.created` through the same EventBus.

## Events

| Event | Emitted when |
|---|---|
| `run.advanced` | a run steps forward |
| `run.completed` | a run reaches `completed` |
| `run.failed` | a run is failed |
| `auth.user.created` | better-auth creates a user |
| `auth.session.created` | better-auth creates a session |

## Environment

```sh
DATABASE_URL=postgres://user:pass@localhost:5432/betterauth
BETTER_AUTH_SECRET=<32+ char random secret>     # openssl rand -base64 32
BETTER_AUTH_URL=http://localhost:3000
TRUSTED_ORIGINS=http://localhost:3001
SMTP_HOST=localhost                             # verification / reset emails (e.g. Mailpit)
SMTP_PORT=1025
MAIL_FROM=noreply@example.com
RABBITMQ_URL=amqp://localhost:5672              # optional; defaults shown
KAFKA_BROKERS=localhost:9092
```

> `buildAuthOptions()` includes `databaseHooks: {}` — required by `@thallesp/nestjs-better-auth` so it can wire the discovered `@DatabaseHook` providers. Removing it makes the app throw on boot.

## Develop

```sh
pnpm --filter @repo/db build     # schema comes from @repo/db (a built package)
pnpm --filter backend dev        # watch mode on :3000
pnpm --filter backend test       # unit tests (state machine, EventBus, hooks, controller)
pnpm --filter backend build
```

Schema and migrations live in [`@repo/db`](../../packages/db); the run/workflow tables are applied with `drizzle-kit migrate` from there.

## Key files

- `src/runs/` — state machine, service, controller
- `src/events/` — `EventBusService`, RMQ client + kafkajs producer wiring
- `src/auth/auth-events.hook.ts` — the auth-event producer
- `src/auth/auth-options.ts` — shared better-auth config
