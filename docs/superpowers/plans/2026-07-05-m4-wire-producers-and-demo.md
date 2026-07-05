# Milestone 4 — Wire Real Producers + Demo — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the real NestJS backend the live producer to both brokers — plain-JSON to Kafka (what the Go consumer parses), Nest-enveloped to RabbitMQ (what the notifier parses) — add auth lifecycle events as a second producer via a better-auth database hook, and ship a `scripts/demo.sh` that lights up both pipelines from one command.

**Architecture:** `EventBusService` fans each domain event to a raw **kafkajs `Producer`** (value = `JSON.stringify(event)`, key = workflowId, falling back to userId) and to the existing RabbitMQ `ClientProxy` (Nest envelope). A `@DatabaseHook()` provider publishes `auth.user.created` / `auth.session.created`; `AuthModule` auto-discovers it. `docker-compose.yml` gains Mailpit so sign-up (which fires the user hook) completes. `scripts/demo.sh` brings up infra, starts backend + notifier + audit-go against the isolated demo DB, drives `/runs/:id/advance`, triggers a sign-up, and prints `notification`, `audit_log`, and `workflow_run_counts`.

**Tech Stack:** NestJS 11, kafkajs (raw producer), `@nestjs/microservices` RMQ (producer), better-auth database hooks, Docker Compose (+ Mailpit), Go audit service.

## Global Constraints

- The **Kafka producer is raw kafkajs** emitting plain JSON (the Go `event.Event` contract); the **RabbitMQ producer stays a Nest `ClientProxy`** (the notifier consumes the Nest `{pattern,data}` envelope).
- Publishing remains fire-and-forget: a down broker logs, never fails the request/hook.
- Auth events flow through the *same* `EventBusService`; they carry `userId` and no run fields. The Go consumer tolerates them (empty workflow/run → audit row with NULLs, `IsCompletion()` false → no count).
- The demo runs everything against the compose demo Postgres (5433); the developer's 5432 DB is never used.
- `@repo/db` must be built (`dist/`) before the backend/notifier run.
- Backend runs run endpoints as `@Public`, so no auth is needed to drive `/runs`.
- Jest keeps mapping `^@repo/db$` to source; backend/notifier unit suites must stay green.

---

### Task 1: Generalize the event contract + raw kafkajs Kafka producer

**Files:**
- Modify: `apps/backend/src/events/event-bus.constants.ts`
- Modify: `apps/backend/src/events/event-bus.service.ts`
- Modify: `apps/backend/src/events/event-bus.service.spec.ts`
- Modify: `apps/backend/src/events/events.module.ts`
- Modify: `apps/backend/src/runs/runs.service.ts` (rename type usage)

**Interfaces:**
- Produces:
  - `EVENTS` gains `AUTH_USER_CREATED='auth.user.created'`, `AUTH_SESSION_CREATED='auth.session.created'`.
  - `DomainEvent{ event: EventName; at: string; runId?, workflowId?, status?, userId?: string; step?: number }` (replaces `RunEventPayload`).
  - `KAFKA_PRODUCER` token (a kafkajs `Producer`).
  - `EventBusService.publish(event: DomainEvent): Promise<void>` — RMQ emit + kafkajs send.

- [ ] **Step 1: Generalize constants**

Replace `apps/backend/src/events/event-bus.constants.ts` with:
```ts
// DI tokens for the two producer transports.
export const RABBITMQ_CLIENT = 'RABBITMQ_CLIENT';
export const KAFKA_PRODUCER = 'KAFKA_PRODUCER';

// Logical event names. On RabbitMQ these are the topic-exchange routing keys.
export const EVENTS = {
  RUN_ADVANCED: 'run.advanced',
  RUN_COMPLETED: 'run.completed',
  RUN_FAILED: 'run.failed',
  AUTH_USER_CREATED: 'auth.user.created',
  AUTH_SESSION_CREATED: 'auth.session.created',
} as const;

export type EventName = (typeof EVENTS)[keyof typeof EVENTS];

// The single payload published to BOTH brokers. Run events fill the run fields;
// auth events fill userId. `event` and `at` are always present.
export interface DomainEvent {
  event: EventName;
  at: string; // ISO-8601
  runId?: string;
  workflowId?: string;
  status?: string;
  step?: number;
  userId?: string;
}
```

- [ ] **Step 2: Update the failing EventBus test**

Replace `apps/backend/src/events/event-bus.service.spec.ts` with:
```ts
import { of, throwError } from 'rxjs';
import { EventBusService } from './event-bus.service';
import { EVENTS, DomainEvent } from './event-bus.constants';

const runEvent: DomainEvent = {
  event: EVENTS.RUN_COMPLETED,
  runId: 'r1',
  workflowId: 'w1',
  status: 'completed',
  step: 2,
  at: '2026-07-05T00:00:00.000Z',
};

describe('EventBusService.publish', () => {
  it('emits to RabbitMQ (event name) and sends plain JSON to Kafka keyed by workflowId', async () => {
    const rabbit = { emit: jest.fn().mockReturnValue(of(undefined)) };
    const producer = { send: jest.fn().mockResolvedValue(undefined) };
    const bus = new EventBusService(rabbit as any, producer as any);

    await bus.publish(runEvent);

    expect(rabbit.emit).toHaveBeenCalledWith('run.completed', runEvent);
    expect(producer.send).toHaveBeenCalledWith({
      topic: 'run-events',
      messages: [{ key: 'w1', value: JSON.stringify(runEvent) }],
    });
  });

  it('keys auth events by userId when there is no workflowId', async () => {
    const authEvent: DomainEvent = {
      event: EVENTS.AUTH_USER_CREATED,
      userId: 'u1',
      at: '2026-07-05T00:00:00.000Z',
    };
    const rabbit = { emit: jest.fn().mockReturnValue(of(undefined)) };
    const producer = { send: jest.fn().mockResolvedValue(undefined) };
    const bus = new EventBusService(rabbit as any, producer as any);

    await bus.publish(authEvent);

    expect(producer.send).toHaveBeenCalledWith({
      topic: 'run-events',
      messages: [{ key: 'u1', value: JSON.stringify(authEvent) }],
    });
  });

  it('does not throw when a broker is down', async () => {
    const rabbit = { emit: jest.fn().mockReturnValue(throwError(() => new Error('ECONNREFUSED'))) };
    const producer = { send: jest.fn().mockRejectedValue(new Error('kafka down')) };
    const bus = new EventBusService(rabbit as any, producer as any);

    await expect(bus.publish(runEvent)).resolves.toBeUndefined();
  });
});
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `pnpm --filter backend test event-bus.service`
Expected: FAIL (constructor now takes a producer; `send` assertions unmet).

- [ ] **Step 4: Rewrite EventBusService**

Replace `apps/backend/src/events/event-bus.service.ts` with:
```ts
import {
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import type { Producer } from 'kafkajs';
import {
  DomainEvent,
  KAFKA_PRODUCER,
  RABBITMQ_CLIENT,
} from './event-bus.constants';

@Injectable()
export class EventBusService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(EventBusService.name);

  constructor(
    @Inject(RABBITMQ_CLIENT) private readonly rabbit: ClientProxy,
    @Inject(KAFKA_PRODUCER) private readonly kafka: Producer,
  ) {}

  async onModuleInit(): Promise<void> {
    try {
      await this.kafka.connect();
    } catch (err) {
      // Best effort: send() will reconnect on demand if the broker returns.
      this.logger.warn(`kafka producer connect deferred: ${(err as Error).message}`);
    }
  }

  async onModuleDestroy(): Promise<void> {
    try {
      await this.kafka.disconnect();
    } catch {
      // ignore shutdown errors
    }
  }

  // Fan the event to both brokers. Neither failure propagates to the caller.
  async publish(event: DomainEvent): Promise<void> {
    await Promise.all([this.toRabbit(event), this.toKafka(event)]);
  }

  private async toRabbit(event: DomainEvent): Promise<void> {
    try {
      await firstValueFrom(this.rabbit.emit(event.event, event));
    } catch (err) {
      this.logger.error(`rabbitmq publish failed: ${(err as Error).message}`);
    }
  }

  private async toKafka(event: DomainEvent): Promise<void> {
    try {
      await this.kafka.send({
        topic: 'run-events',
        messages: [{ key: event.workflowId ?? event.userId ?? null, value: JSON.stringify(event) }],
      });
    } catch (err) {
      this.logger.error(`kafka publish failed: ${(err as Error).message}`);
    }
  }
}
```

- [ ] **Step 5: Rewrite EventsModule (RMQ client + kafkajs producer)**

Replace `apps/backend/src/events/events.module.ts` with:
```ts
import { Global, Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { Kafka } from 'kafkajs';
import { KAFKA_PRODUCER, RABBITMQ_CLIENT } from './event-bus.constants';
import { EventBusService } from './event-bus.service';

@Global()
@Module({
  imports: [
    ClientsModule.register([
      {
        name: RABBITMQ_CLIENT,
        transport: Transport.RMQ,
        options: {
          urls: [process.env.RABBITMQ_URL ?? 'amqp://localhost:5672'],
          exchange: 'events',
          exchangeType: 'topic',
          wildcards: true,
          queue: 'backend-producer-unused',
        },
      },
    ]),
  ],
  providers: [
    {
      // Raw kafkajs producer: the Kafka consumer is a Go service that expects a
      // plain-JSON value, so we bypass Nest's Kafka transport envelope.
      provide: KAFKA_PRODUCER,
      useFactory: () => {
        const kafka = new Kafka({
          clientId: 'backend-producer',
          brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
        });
        return kafka.producer();
      },
    },
    EventBusService,
  ],
  exports: [EventBusService],
})
export class EventsModule {}
```

- [ ] **Step 6: Update RunsService to the renamed type**

In `apps/backend/src/runs/runs.service.ts`, no field changes are needed (it already passes `event, runId, workflowId, status, step, at`). If it imports `RunEventPayload` anywhere, it does not — it builds the object inline. No change required beyond confirming the build. (If a future compile error names `RunEventPayload`, replace it with `DomainEvent`.)

- [ ] **Step 7: Run tests, typecheck, build**

Run:
```bash
pnpm --filter backend test && pnpm --filter backend exec tsc --noEmit && pnpm --filter backend build
```
Expected: all suites pass; tsc clean; `dist/main.js` present.

- [ ] **Step 8: Commit**

```bash
git add apps/backend/src/events apps/backend/src/runs/runs.service.ts
git commit -m "feat(backend): raw kafkajs producer with plain-JSON contract; auth event types"
```

---

### Task 2: Auth-event producer (better-auth database hook)

**Files:**
- Create: `apps/backend/src/auth/auth-events.hook.ts`
- Test: `apps/backend/src/auth/auth-events.hook.spec.ts`
- Modify: `apps/backend/src/app.module.ts` (register the provider)

**Interfaces:**
- Consumes: `EventBusService`, `EVENTS`, `@DatabaseHook`/`@AfterCreate` from `@thallesp/nestjs-better-auth`.
- Produces: `AuthEventsHook` provider publishing `auth.user.created` / `auth.session.created`.

- [ ] **Step 1: Write the failing test**

Create `apps/backend/src/auth/auth-events.hook.spec.ts`:
```ts
// The decorators come from an ESM-only package; stub them so ts-jest can load.
jest.mock('@thallesp/nestjs-better-auth', () => ({
  DatabaseHook: () => () => undefined,
  AfterCreate: () => () => undefined,
}));

import { AuthEventsHook } from './auth-events.hook';
import { EVENTS } from '../events/event-bus.constants';

describe('AuthEventsHook', () => {
  it('publishes auth.user.created with the new user id', async () => {
    const events = { publish: jest.fn().mockResolvedValue(undefined) };
    const hook = new AuthEventsHook(events as any);

    await hook.onUserCreated({ id: 'u1', email: 'a@b.c' });

    expect(events.publish).toHaveBeenCalledWith(
      expect.objectContaining({ event: EVENTS.AUTH_USER_CREATED, userId: 'u1' }),
    );
  });

  it('publishes auth.session.created with the session user id', async () => {
    const events = { publish: jest.fn().mockResolvedValue(undefined) };
    const hook = new AuthEventsHook(events as any);

    await hook.onSessionCreated({ id: 's1', userId: 'u1' });

    expect(events.publish).toHaveBeenCalledWith(
      expect.objectContaining({ event: EVENTS.AUTH_SESSION_CREATED, userId: 'u1' }),
    );
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --filter backend test auth-events.hook`
Expected: FAIL — cannot find module `./auth-events.hook`.

- [ ] **Step 3: Write the hook**

Create `apps/backend/src/auth/auth-events.hook.ts`:
```ts
import { Injectable } from '@nestjs/common';
import { AfterCreate, DatabaseHook } from '@thallesp/nestjs-better-auth';
import { EVENTS } from '../events/event-bus.constants';
import { EventBusService } from '../events/event-bus.service';

// Second producer: better-auth database lifecycle events flow through the same
// EventBus as run events. AuthModule auto-discovers @DatabaseHook providers.
@Injectable()
@DatabaseHook()
export class AuthEventsHook {
  constructor(private readonly events: EventBusService) {}

  @AfterCreate('user')
  async onUserCreated(user: { id?: string }): Promise<void> {
    await this.events.publish({
      event: EVENTS.AUTH_USER_CREATED,
      userId: user?.id,
      at: new Date().toISOString(),
    });
  }

  @AfterCreate('session')
  async onSessionCreated(session: { userId?: string }): Promise<void> {
    await this.events.publish({
      event: EVENTS.AUTH_SESSION_CREATED,
      userId: session?.userId,
      at: new Date().toISOString(),
    });
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pnpm --filter backend test auth-events.hook`
Expected: PASS (2 tests).

- [ ] **Step 5: Register the hook provider**

In `apps/backend/src/app.module.ts`, add the import:
```ts
import { AuthEventsHook } from './auth/auth-events.hook';
```
and add `AuthEventsHook` to the `providers` array (beside the existing `APP_GUARD` provider):
```ts
  providers: [
    AuthEventsHook,
    {
      provide: APP_GUARD,
      useClass: AuthGuard,
    },
  ],
```

- [ ] **Step 6: Full backend verification**

Run:
```bash
pnpm --filter backend test && pnpm --filter backend exec tsc --noEmit && pnpm --filter backend build
```
Expected: all green (13 test files now); build emits `dist/main.js`.

- [ ] **Step 7: Commit**

```bash
git add apps/backend/src/auth/auth-events.hook.ts apps/backend/src/auth/auth-events.hook.spec.ts apps/backend/src/app.module.ts
git commit -m "feat(backend): publish auth.user.created/session.created via database hook"
```

---

### Task 3: Mailpit in compose + the demo script + end-to-end

**Files:**
- Modify: `docker-compose.yml` (add `mailpit`)
- Create: `scripts/demo.sh`

- [ ] **Step 1: Add Mailpit to compose**

In `docker-compose.yml`, add under `services:`:
```yaml
  mailpit:
    image: axllent/mailpit:latest
    ports:
      - "1025:1025"
      - "8025:8025"
```

- [ ] **Step 2: Write the demo script**

Create `scripts/demo.sh`:
```bash
#!/usr/bin/env bash
# Drives both pipelines end-to-end against the isolated demo stack.
set -euo pipefail
cd "$(dirname "$0")/.."

DEMO_DB="postgres://postgres:postgres@localhost:5433/betterauth"
DEMO_DB_SSL="${DEMO_DB}?sslmode=disable"
export DATABASE_URL="$DEMO_DB"
export RABBITMQ_URL="amqp://localhost:5672"
export KAFKA_BROKERS="localhost:9092"
export BETTER_AUTH_SECRET="${BETTER_AUTH_SECRET:-demo-secret-please-change}"
export BETTER_AUTH_URL="http://localhost:3000"
export TRUSTED_ORIGINS="http://localhost:3000"
export SMTP_HOST="localhost"
export SMTP_PORT="1025"
export MAIL_FROM="demo@example.com"
export PORT=3000

pids=()
cleanup() {
  echo "--- cleanup ---"
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  docker rm -f demo-audit >/dev/null 2>&1 || true
  docker compose down >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== infra up ==="
docker compose up -d
until [ "$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q kafka)")" = healthy ]; do sleep 3; done
until [ "$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q postgres-demo)")" = healthy ]; do sleep 2; done
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic run-events --partitions 3 --replication-factor 1 2>/dev/null || true
sleep 4
DATABASE_URL="$DEMO_DB" pnpm --filter @repo/db exec drizzle-kit migrate
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "truncate audit_log, workflow_run_counts, notification;" || true

echo "=== build ==="
pnpm --filter @repo/db build
pnpm --filter backend build
pnpm --filter notifier build

echo "=== start backend + notifier (host) and audit-go (container) ==="
node apps/backend/dist/main.js & pids+=("$!")
node apps/notifier/dist/main.js & pids+=("$!")
docker rm -f demo-audit >/dev/null 2>&1 || true
docker run -d --name demo-audit --network better-auth_default \
  -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache-25:/go/pkg/mod golang:1.25 \
  go run ./cmd/audit --lib kafka-go --from-beginning --group demo-audit --brokers kafka:29092 \
  --snapshot-every 2s --database-url "postgres://postgres:postgres@postgres-demo:5432/betterauth?sslmode=disable" >/dev/null
sleep 8

echo "=== drive the run pipeline ==="
RUN_ID=$(curl -s -X POST http://localhost:3000/runs -H 'content-type: application/json' -d '{"workflow":"demo"}' | sed -E 's/.*"id":"([^"]+)".*/\1/')
echo "created run $RUN_ID"
curl -s -X POST "http://localhost:3000/runs/$RUN_ID/advance" >/dev/null   # queued -> running (run.advanced)
curl -s -X POST "http://localhost:3000/runs/$RUN_ID/advance" >/dev/null   # running -> completed (run.advanced + run.completed)

echo "=== trigger an auth event (sign-up) ==="
curl -s -X POST http://localhost:3000/api/auth/sign-up/email -H 'content-type: application/json' \
  -d '{"email":"demo+'"$RANDOM"'@example.com","password":"demo-password-123","name":"Demo"}' >/dev/null || true

sleep 6

echo "=== RabbitMQ pipeline: notification rows ==="
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "select type, run_id from notification order by created_at;"
echo "=== Kafka pipeline: audit_log ==="
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "select event_type, workflow_id, kafka_partition, kafka_offset from audit_log order by kafka_partition, kafka_offset;"
echo "=== Kafka pipeline: workflow_run_counts ==="
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "select workflow_id, completed_count from workflow_run_counts;"
echo "=== replay from offset 0 (reconstruct counts) ==="
docker run --rm --network better-auth_default -v "$PWD/apps/audit-go":/src -w /src \
  -v audit-go-cache-25:/go/pkg/mod golang:1.25 \
  go run ./cmd/audit --replay --lib sarama --brokers kafka:29092 --replay-idle 5s 2>&1 | grep -E "rebuilt|  " | tail -6

echo "=== demo complete ==="
```

- [ ] **Step 3: Make it executable and run it**

Run:
```bash
chmod +x scripts/demo.sh
./scripts/demo.sh
```
Expected: `notification` holds `run.advanced`, `run.completed`, and `auth.user.created` (and likely `auth.session.created`); `audit_log` holds the run events (with partition/offset) plus the auth event(s); `workflow_run_counts` shows the demo workflow with `completed_count = 1`; replay reprints the reconstructed count. (Debug and adjust timings/ports as needed — this is a live integration.)

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml scripts/demo.sh
git commit -m "feat: mailpit in compose and end-to-end demo script"
```

---

## Definition of done (Milestone 4)

- Backend publishes plain-JSON to Kafka (Go-parseable) and Nest-enveloped to RabbitMQ (notifier-parseable), fire-and-forget.
- `auth.user.created` / `auth.session.created` publish through the same EventBus via a database hook; backend unit suites green.
- `scripts/demo.sh` drives `/runs/:id/advance` + a sign-up and shows both pipelines populated (`notification`, `audit_log`, `workflow_run_counts`) plus a replay.

## The whole thing, in one command

`./scripts/demo.sh` — one run advance fans to two consumers: RabbitMQ pushes to the notifier (ack / requeue / DLQ, ephemeral); Kafka persists to the log the Go service pulls, partitions, and can **replay from offset 0**. Auth sign-up rides the same rails as a second producer. That is the complete RabbitMQ-vs-Kafka contrast, running.
