# Milestone 2b — Notifier (RabbitMQ Consumer) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `apps/notifier`, a NestJS RabbitMQ microservice that consumes events off a topic exchange, writes `notification` rows via `@repo/db`, and demonstrates the smart-broker lesson: manual ack, one bounded requeue on failure, then dead-letter to a DLQ.

**Architecture:** The backend (M1 producer) publishes to topic exchange `events` (routing key = event name). The notifier binds queue `notifications` to that exchange with `run.#` and `auth.#`, consuming with **manual ack** (`noAck: false`). Nest owns the main exchange/queue/bindings; a small amqplib bootstrap owns the dead-letter side (`events.dlx` → `notifications.dlq`) that Nest knows nothing about. On a processing failure the handler requeues once (via the `redelivered` flag) and dead-letters on the second attempt. A root `docker-compose.yml` runs RabbitMQ plus an **isolated demo Postgres on port 5433** (the developer's own Postgres on 5432 is never touched), enabling a real end-to-end verification of the ack/DLQ behavior.

**Tech Stack:** NestJS 11 microservice (`@nestjs/microservices` RMQ transport), `amqplib`, Drizzle via `@repo/db`, Docker Compose (RabbitMQ 3-management, Postgres 16), Jest.

## Global Constraints

- pnpm workspace; new app at `apps/notifier` (picked up by `apps/*`).
- The notifier is a **pure microservice** (`NestFactory.createMicroservice`) — no HTTP server.
- Manual ack only (`noAck: false`); the handler acks/nacks explicitly. Never rely on auto-ack.
- Retry policy: **one requeue, then dead-letter** — first failure `nack(requeue=true)`; a `redelivered` failure `nack(requeue=false)` → `events.dlx` → `notifications.dlq`.
- Nest asserts exchange `events` (topic) + queue `notifications` (with `x-dead-letter-exchange: events.dlx`) + `run.#`/`auth.#` bindings. The amqplib bootstrap asserts ONLY `events.dlx` + `notifications.dlq` + their binding.
- `@repo/db` types used in decorated constructor params must be `import type` (isolatedModules + emitDecoratorMetadata).
- Jest maps `^@repo/db$` to package source (same pattern as backend).
- Demo Postgres: `postgres://postgres:postgres@localhost:5433/betterauth`. RabbitMQ: `amqp://localhost:5672`, management UI `http://localhost:15672` (guest/guest).
- Do NOT run migrations against the developer's `localhost:5432` DB. Only the compose demo DB on 5433.

---

### Task 1: Scaffold the notifier app + compose infra

**Files:**
- Create: `apps/notifier/package.json`
- Create: `apps/notifier/tsconfig.json`
- Create: `apps/notifier/tsconfig.build.json`
- Create: `apps/notifier/nest-cli.json`
- Create: `apps/notifier/src/app.module.ts`
- Create: `apps/notifier/src/main.ts` (placeholder bootstrap, finalized in Task 4)
- Create: `apps/notifier/src/database/database-connection.ts`
- Create: `apps/notifier/src/database/database.module.ts`
- Create: `docker-compose.yml` (repo root)

**Interfaces:**
- Consumes: `@repo/db` (`createDatabase`).
- Produces: a buildable `notifier` app and a `DATABASE_CONNECTION` provider; a compose stack (`rabbitmq`, `postgres-demo`).

- [ ] **Step 1: Package manifest**

Create `apps/notifier/package.json`:
```json
{
  "name": "notifier",
  "version": "0.0.1",
  "private": true,
  "scripts": {
    "build": "nest build",
    "start": "nest start",
    "dev": "nest start --watch",
    "start:prod": "node dist/main",
    "test": "jest",
    "publish:test": "ts-node scripts/publish-test-event.ts"
  },
  "dependencies": {
    "@repo/db": "workspace:*",
    "@nestjs/common": "^11.0.1",
    "@nestjs/core": "^11.0.1",
    "@nestjs/microservices": "^11.1.27",
    "amqp-connection-manager": "^4",
    "amqplib": "^0.10",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1"
  },
  "devDependencies": {
    "@nestjs/cli": "^11.0.0",
    "@nestjs/schematics": "^11.0.0",
    "@nestjs/testing": "^11.0.1",
    "@types/amqplib": "^0.10",
    "@types/jest": "^30.0.0",
    "@types/node": "^24.0.0",
    "jest": "^30.0.0",
    "ts-jest": "^29.2.5",
    "ts-node": "^10.9.2",
    "typescript": "^5.7.3"
  },
  "jest": {
    "moduleFileExtensions": ["js", "json", "ts"],
    "rootDir": "src",
    "testRegex": ".*\\.spec\\.ts$",
    "transform": { "^.+\\.(t|j)s$": "ts-jest" },
    "moduleNameMapper": {
      "^@repo/db$": "<rootDir>/../../../packages/db/src/index.ts"
    },
    "testEnvironment": "node"
  }
}
```

- [ ] **Step 2: TypeScript config** (mirrors the backend)

Create `apps/notifier/tsconfig.json`:
```json
{
  "compilerOptions": {
    "module": "nodenext",
    "moduleResolution": "nodenext",
    "resolvePackageJsonExports": true,
    "esModuleInterop": true,
    "isolatedModules": true,
    "declaration": false,
    "removeComments": true,
    "emitDecoratorMetadata": true,
    "experimentalDecorators": true,
    "allowSyntheticDefaultImports": true,
    "target": "ES2023",
    "sourceMap": true,
    "outDir": "./dist",
    "baseUrl": "./",
    "incremental": true,
    "skipLibCheck": true,
    "strictNullChecks": true,
    "forceConsistentCasingInFileNames": true,
    "noImplicitAny": false,
    "strictBindCallApply": false,
    "noFallthroughCasesInSwitch": false
  }
}
```

Create `apps/notifier/tsconfig.build.json`:
```json
{
  "extends": "./tsconfig.json",
  "exclude": ["node_modules", "test", "dist", "scripts", "**/*spec.ts"]
}
```

Create `apps/notifier/nest-cli.json`:
```json
{
  "$schema": "https://json.schemastore.org/nest-cli",
  "collection": "@nestjs/schematics",
  "sourceRoot": "src",
  "compilerOptions": {
    "deleteOutDir": true
  }
}
```

- [ ] **Step 3: Database provider**

Create `apps/notifier/src/database/database-connection.ts`:
```ts
export const DATABASE_CONNECTION = 'database_connection';
```

Create `apps/notifier/src/database/database.module.ts`:
```ts
import { Module } from '@nestjs/common';
import { createDatabase } from '@repo/db';
import { DATABASE_CONNECTION } from './database-connection';

@Module({
  providers: [
    {
      provide: DATABASE_CONNECTION,
      useFactory: () => {
        const url = process.env.DATABASE_URL;
        if (!url) throw new Error('DATABASE_URL is required');
        return createDatabase(url);
      },
    },
  ],
  exports: [DATABASE_CONNECTION],
})
export class DatabaseModule {}
```

- [ ] **Step 4: Placeholder module and bootstrap**

Create `apps/notifier/src/app.module.ts`:
```ts
import { Module } from '@nestjs/common';
import { DatabaseModule } from './database/database.module';

@Module({
  imports: [DatabaseModule],
})
export class AppModule {}
```

Create `apps/notifier/src/main.ts` (finalized in Task 4):
```ts
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

// Replaced in Task 4 with the RMQ microservice bootstrap.
async function bootstrap() {
  await NestFactory.createApplicationContext(AppModule);
}
bootstrap();
```

- [ ] **Step 5: Compose stack**

Create `docker-compose.yml` at the repo root:
```yaml
services:
  rabbitmq:
    image: rabbitmq:3-management
    ports:
      - "5672:5672"
      - "15672:15672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 5s
      timeout: 5s
      retries: 10

  postgres-demo:
    image: postgres:16
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: betterauth
    ports:
      - "5433:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 10
```

- [ ] **Step 6: Install, build, validate compose**

Run:
```bash
cd /Users/nocturnal/Gitlab/better-auth
pnpm install
pnpm --filter notifier build
docker compose config >/dev/null && echo "compose OK"
```
Expected: install links `notifier`; `nest build` emits `apps/notifier/dist/main.js`; compose config validates.

- [ ] **Step 7: Commit**

```bash
git add apps/notifier/package.json apps/notifier/tsconfig.json apps/notifier/tsconfig.build.json apps/notifier/nest-cli.json apps/notifier/src docker-compose.yml pnpm-lock.yaml
git commit -m "chore(notifier): scaffold RMQ microservice app and compose infra"
```

---

### Task 2: RabbitMQ constants & retry policy (TDD)

**Files:**
- Create: `apps/notifier/src/rabbit/rabbit.constants.ts`
- Create: `apps/notifier/src/rabbit/retry-policy.ts`
- Test: `apps/notifier/src/rabbit/retry-policy.spec.ts`

**Interfaces:**
- Produces:
  - Constants `EVENTS_EXCHANGE`, `DLX_EXCHANGE`, `NOTIFICATIONS_QUEUE`, `NOTIFICATIONS_DLQ`.
  - `type AckAction = 'ack' | 'requeue' | 'deadletter'`
  - `function decideAction(succeeded: boolean, redelivered: boolean): AckAction`

- [ ] **Step 1: Write the failing test**

Create `apps/notifier/src/rabbit/retry-policy.spec.ts`:
```ts
import { decideAction } from './retry-policy';

describe('decideAction', () => {
  it('acks a successful message', () => {
    expect(decideAction(true, false)).toBe('ack');
    expect(decideAction(true, true)).toBe('ack');
  });

  it('requeues the first failure', () => {
    expect(decideAction(false, false)).toBe('requeue');
  });

  it('dead-letters a failure that was already redelivered', () => {
    expect(decideAction(false, true)).toBe('deadletter');
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --filter notifier test retry-policy`
Expected: FAIL — cannot find module `./retry-policy`.

- [ ] **Step 3: Write the implementations**

Create `apps/notifier/src/rabbit/rabbit.constants.ts`:
```ts
export const EVENTS_EXCHANGE = 'events';
export const DLX_EXCHANGE = 'events.dlx';
export const NOTIFICATIONS_QUEUE = 'notifications';
export const NOTIFICATIONS_DLQ = 'notifications.dlq';
```

Create `apps/notifier/src/rabbit/retry-policy.ts`:
```ts
export type AckAction = 'ack' | 'requeue' | 'deadletter';

// One bounded retry: a fresh failure is requeued once; a failure that was
// already redelivered is dead-lettered. RabbitMQ's `redelivered` flag is the
// only state we need — no external attempt counter.
export function decideAction(succeeded: boolean, redelivered: boolean): AckAction {
  if (succeeded) return 'ack';
  return redelivered ? 'deadletter' : 'requeue';
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pnpm --filter notifier test retry-policy`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add apps/notifier/src/rabbit
git commit -m "feat(notifier): add rabbit constants and retry policy"
```

---

### Task 3: NotificationService (TDD)

**Files:**
- Create: `apps/notifier/src/notifications/notification.service.ts`
- Test: `apps/notifier/src/notifications/notification.service.spec.ts`

**Interfaces:**
- Consumes: `DATABASE_CONNECTION`, `notification` table + `Database` type from `@repo/db`.
- Produces:
  - `interface IncomingEvent { event: string; runId?: string | null; workflowId?: string | null; status?: string; step?: number; at?: string }`
  - `class NotificationService { record(event: IncomingEvent): Promise<void> }`

- [ ] **Step 1: Write the failing test**

Create `apps/notifier/src/notifications/notification.service.spec.ts`:
```ts
import { NotificationService } from './notification.service';

describe('NotificationService.record', () => {
  it('inserts a notification row derived from the event', async () => {
    const values = jest.fn().mockResolvedValue(undefined);
    const db = { insert: jest.fn().mockReturnValue({ values }) };
    const service = new NotificationService(db as any);

    await service.record({ event: 'run.advanced', runId: 'r1', step: 1 });

    expect(db.insert).toHaveBeenCalledTimes(1);
    expect(values).toHaveBeenCalledWith(
      expect.objectContaining({
        runId: 'r1',
        type: 'run.advanced',
        payload: expect.objectContaining({ event: 'run.advanced' }),
      }),
    );
  });

  it('stores a null runId when the event has none (auth events)', async () => {
    const values = jest.fn().mockResolvedValue(undefined);
    const db = { insert: jest.fn().mockReturnValue({ values }) };
    const service = new NotificationService(db as any);

    await service.record({ event: 'auth.user.created' });

    expect(values).toHaveBeenCalledWith(
      expect.objectContaining({ runId: null, type: 'auth.user.created' }),
    );
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --filter notifier test notification.service`
Expected: FAIL — cannot find module `./notification.service`.

- [ ] **Step 3: Write the implementation**

Create `apps/notifier/src/notifications/notification.service.ts`:
```ts
import { Inject, Injectable } from '@nestjs/common';
import { notification } from '@repo/db';
import type { Database } from '@repo/db';
import { DATABASE_CONNECTION } from '../database/database-connection';

export interface IncomingEvent {
  event: string;
  runId?: string | null;
  workflowId?: string | null;
  status?: string;
  step?: number;
  at?: string;
}

@Injectable()
export class NotificationService {
  constructor(@Inject(DATABASE_CONNECTION) private readonly db: Database) {}

  // Persist the consumed event as a notification row. runId is optional:
  // auth events carry none.
  async record(event: IncomingEvent): Promise<void> {
    await this.db.insert(notification).values({
      runId: event.runId ?? null,
      type: event.event,
      payload: event,
    });
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pnpm --filter notifier test notification.service`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add apps/notifier/src/notifications/notification.service.ts apps/notifier/src/notifications/notification.service.spec.ts
git commit -m "feat(notifier): add NotificationService"
```

---

### Task 4: Consumer controller, topology, and bootstrap (TDD for the handler)

**Files:**
- Create: `apps/notifier/src/rabbit/topology.ts`
- Create: `apps/notifier/src/notifications/notifications.controller.ts`
- Test: `apps/notifier/src/notifications/notifications.controller.spec.ts`
- Modify: `apps/notifier/src/app.module.ts`
- Modify: `apps/notifier/src/main.ts`

**Interfaces:**
- Consumes: `decideAction`, `NotificationService`, rabbit constants.
- Produces: `NotificationsController` with `run.#` + `auth.#` `@EventPattern` handlers doing manual ack/nack; `assertDeadLetterTopology(url)`; the finalized RMQ bootstrap.

- [ ] **Step 1: Write the failing test for the handler**

Create `apps/notifier/src/notifications/notifications.controller.spec.ts`:
```ts
import { NotificationsController } from './notifications.controller';

function ctx(routingKey: string, redelivered: boolean) {
  const message = { fields: { routingKey, redelivered } };
  const channel = { ack: jest.fn(), nack: jest.fn() };
  return {
    getChannelRef: () => channel,
    getMessage: () => message,
    _channel: channel,
    _message: message,
  } as any;
}

describe('NotificationsController.consume', () => {
  it('records and acks on success', async () => {
    const service = { record: jest.fn().mockResolvedValue(undefined) };
    const controller = new NotificationsController(service as any);
    const context = ctx('run.advanced', false);

    await controller.handleRunEvent({ event: 'run.advanced' }, context);

    expect(service.record).toHaveBeenCalledWith({ event: 'run.advanced' });
    expect(context._channel.ack).toHaveBeenCalledWith(context._message);
    expect(context._channel.nack).not.toHaveBeenCalled();
  });

  it('requeues once when processing throws on first delivery', async () => {
    const service = { record: jest.fn().mockRejectedValue(new Error('boom')) };
    const controller = new NotificationsController(service as any);
    const context = ctx('run.advanced', false);

    await controller.handleRunEvent({ event: 'run.advanced' }, context);

    expect(context._channel.nack).toHaveBeenCalledWith(context._message, false, true);
  });

  it('dead-letters when processing throws on a redelivered message', async () => {
    const service = { record: jest.fn().mockRejectedValue(new Error('boom')) };
    const controller = new NotificationsController(service as any);
    const context = ctx('run.advanced', true);

    await controller.handleRunEvent({ event: 'run.advanced' }, context);

    expect(context._channel.nack).toHaveBeenCalledWith(context._message, false, false);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --filter notifier test notifications.controller`
Expected: FAIL — cannot find module `./notifications.controller`.

- [ ] **Step 3: Write the controller**

Create `apps/notifier/src/notifications/notifications.controller.ts`:
```ts
import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, Payload, RmqContext } from '@nestjs/microservices';
import { decideAction } from '../rabbit/retry-policy';
import { IncomingEvent, NotificationService } from './notification.service';

@Controller()
export class NotificationsController {
  private readonly logger = new Logger(NotificationsController.name);
  // Set NOTIFIER_FAIL_ON=<routingKey> to force failures for the DLQ demo.
  private readonly failOn = process.env.NOTIFIER_FAIL_ON;

  constructor(private readonly notifications: NotificationService) {}

  @EventPattern('run.#')
  handleRunEvent(@Payload() data: IncomingEvent, @Ctx() context: RmqContext) {
    return this.consume(data, context);
  }

  @EventPattern('auth.#')
  handleAuthEvent(@Payload() data: IncomingEvent, @Ctx() context: RmqContext) {
    return this.consume(data, context);
  }

  private async consume(data: IncomingEvent, context: RmqContext): Promise<void> {
    const channel = context.getChannelRef();
    const message = context.getMessage();
    const routingKey: string = message.fields.routingKey;
    const redelivered = Boolean(message.fields.redelivered);

    let succeeded = true;
    try {
      if (this.failOn && routingKey === this.failOn) {
        throw new Error(`simulated failure for ${routingKey}`);
      }
      await this.notifications.record(data);
    } catch (err) {
      succeeded = false;
      this.logger.warn(`processing failed for ${routingKey}: ${(err as Error).message}`);
    }

    switch (decideAction(succeeded, redelivered)) {
      case 'ack':
        channel.ack(message);
        break;
      case 'requeue':
        this.logger.warn(`requeue ${routingKey} (first failure)`);
        channel.nack(message, false, true);
        break;
      case 'deadletter':
        this.logger.error(`dead-letter ${routingKey} (already retried)`);
        channel.nack(message, false, false);
        break;
    }
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pnpm --filter notifier test notifications.controller`
Expected: PASS (3 tests).

- [ ] **Step 5: Write the dead-letter topology**

Create `apps/notifier/src/rabbit/topology.ts`:
```ts
import * as amqp from 'amqplib';
import { DLX_EXCHANGE, NOTIFICATIONS_DLQ } from './rabbit.constants';

// Nest asserts the main `events` exchange and `notifications` queue (with its
// x-dead-letter-exchange argument) plus the run.#/auth.# bindings. It knows
// nothing about the dead-letter side, so we assert that ourselves before the
// microservice starts.
export async function assertDeadLetterTopology(url: string): Promise<void> {
  const connection = await amqp.connect(url);
  const channel = await connection.createChannel();
  await channel.assertExchange(DLX_EXCHANGE, 'fanout', { durable: true });
  await channel.assertQueue(NOTIFICATIONS_DLQ, { durable: true });
  await channel.bindQueue(NOTIFICATIONS_DLQ, DLX_EXCHANGE, '');
  await channel.close();
  await connection.close();
}
```

- [ ] **Step 6: Wire the controller into the module**

Replace `apps/notifier/src/app.module.ts` with:
```ts
import { Module } from '@nestjs/common';
import { DatabaseModule } from './database/database.module';
import { NotificationsController } from './notifications/notifications.controller';
import { NotificationService } from './notifications/notification.service';

@Module({
  imports: [DatabaseModule],
  controllers: [NotificationsController],
  providers: [NotificationService],
})
export class AppModule {}
```

- [ ] **Step 7: Finalize the bootstrap**

Replace `apps/notifier/src/main.ts` with:
```ts
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { assertDeadLetterTopology } from './rabbit/topology';
import {
  DLX_EXCHANGE,
  EVENTS_EXCHANGE,
  NOTIFICATIONS_QUEUE,
} from './rabbit/rabbit.constants';

async function bootstrap() {
  const url = process.env.RABBITMQ_URL ?? 'amqp://localhost:5672';

  // Own the dead-letter side before Nest asserts the main queue/exchange.
  await assertDeadLetterTopology(url);

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.RMQ,
    options: {
      urls: [url],
      exchange: EVENTS_EXCHANGE,
      exchangeType: 'topic',
      wildcards: true,
      queue: NOTIFICATIONS_QUEUE,
      noAck: false,
      queueOptions: {
        durable: true,
        arguments: { 'x-dead-letter-exchange': DLX_EXCHANGE },
      },
    },
  });

  await app.listen();
  new Logger('Bootstrap').log('notifier listening');
}

bootstrap();
```

- [ ] **Step 8: Build and run the full notifier test suite**

Run: `pnpm --filter notifier test && pnpm --filter notifier build`
Expected: all specs PASS; `apps/notifier/dist/main.js` present.

- [ ] **Step 9: Commit**

```bash
git add apps/notifier/src
git commit -m "feat(notifier): consume run.#/auth.# with manual ack, requeue, and DLQ"
```

---

### Task 5: End-to-end verification against real RabbitMQ

This task has no unit test — it is the observable proof of the RabbitMQ lesson,
run against the compose stack. Add the publish helper, then execute the runbook.

**Files:**
- Create: `apps/notifier/scripts/publish-test-event.ts`

- [ ] **Step 1: Add the publish helper**

Create `apps/notifier/scripts/publish-test-event.ts`:
```ts
import * as amqp from 'amqplib';
import { EVENTS_EXCHANGE } from '../src/rabbit/rabbit.constants';

// Publishes a Nest-enveloped message ({ pattern, data }) to the events
// exchange with the given routing key. Usage: ts-node publish-test-event.ts run.advanced
async function main() {
  const url = process.env.RABBITMQ_URL ?? 'amqp://localhost:5672';
  const routingKey = process.argv[2] ?? 'run.advanced';
  const data = {
    event: routingKey,
    runId: process.env.TEST_RUN_ID ?? null,
    workflowId: process.env.TEST_WORKFLOW_ID ?? null,
    status: 'running',
    step: 1,
    at: new Date().toISOString(),
  };
  const connection = await amqp.connect(url);
  const channel = await connection.createChannel();
  await channel.assertExchange(EVENTS_EXCHANGE, 'topic', { durable: true });
  channel.publish(
    EVENTS_EXCHANGE,
    routingKey,
    Buffer.from(JSON.stringify({ pattern: routingKey, data })),
  );
  await channel.close();
  await connection.close();
  console.log(`published ${routingKey}`);
}
main();
```

- [ ] **Step 2: Bring up infra and migrate the demo DB**

Run:
```bash
cd /Users/nocturnal/Gitlab/better-auth
docker compose up -d
# wait until healthy
until [ "$(docker inspect -f '{{.State.Health.Status}}' $(docker compose ps -q rabbitmq))" = healthy ]; do sleep 2; done
until [ "$(docker inspect -f '{{.State.Health.Status}}' $(docker compose ps -q postgres-demo))" = healthy ]; do sleep 2; done
DATABASE_URL=postgres://postgres:postgres@localhost:5433/betterauth pnpm --filter @repo/db exec drizzle-kit migrate
```
Expected: both containers healthy; drizzle-kit applies migrations `0000`–`0002` to the demo DB (tables incl. `notification` created).

- [ ] **Step 3: Start the notifier (success path)**

Run (background):
```bash
RABBITMQ_URL=amqp://localhost:5672 \
DATABASE_URL=postgres://postgres:postgres@localhost:5433/betterauth \
node apps/notifier/dist/main.js &
sleep 3
```
Expected: log line `notifier listening`; queues `notifications` and `notifications.dlq` exist (management UI / API).

- [ ] **Step 4: Publish a success event and verify the row**

Run:
```bash
RABBITMQ_URL=amqp://localhost:5672 pnpm --filter notifier exec ts-node scripts/publish-test-event.ts run.advanced
sleep 2
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "select type, run_id from notification;"
```
Expected: one `notification` row with `type = run.advanced`.

- [ ] **Step 5: Demonstrate requeue → DLQ**

Stop the notifier, restart it with the failure toggle, publish a matching event:
```bash
kill %1 2>/dev/null
NOTIFIER_FAIL_ON=run.completed \
RABBITMQ_URL=amqp://localhost:5672 \
DATABASE_URL=postgres://postgres:postgres@localhost:5433/betterauth \
node apps/notifier/dist/main.js &
sleep 3
RABBITMQ_URL=amqp://localhost:5672 pnpm --filter notifier exec ts-node scripts/publish-test-event.ts run.completed
sleep 3
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/notifications.dlq | python3 -c "import sys,json; print('dlq messages:', json.load(sys.stdin)['messages'])"
```
Expected: notifier logs show `requeue run.completed (first failure)` then `dead-letter run.completed (already retried)`; the DLQ reports `dlq messages: 1`.

- [ ] **Step 6: Tear down**

Run:
```bash
kill %1 2>/dev/null
docker compose down
```

- [ ] **Step 7: Commit**

```bash
git add apps/notifier/scripts/publish-test-event.ts
git commit -m "feat(notifier): add publish helper for end-to-end DLQ demo"
```

---

## Definition of done (Milestone 2b)

- `apps/notifier` builds and its unit tests pass (retry policy, service, controller).
- A message published to `events` with `run.advanced` produces a `notification` row.
- A message that fails processing is requeued once, then dead-lettered to `notifications.dlq` (observed live).
- `docker-compose.yml` runs RabbitMQ + an isolated demo Postgres (5433); the developer's 5432 DB is never migrated against.

## The lesson made concrete

RabbitMQ **pushed** each message to the notifier, which **acked** it per-message; a failing message was **requeued once** then **dead-lettered** — and once acked, a message is **gone** (no replay). This is the smart-broker/ephemeral half of the contrast. M3 (Kafka/Go) builds the replayable-log half.

## Next (Plan 3, separate)

`apps/audit-go` — Kafka consumer group appending to `audit_log`, maintaining a live count map with a periodic snapshot to `workflow_run_counts`, plus the `--replay` from offset 0. Requires adding Kafka to `docker-compose.yml`.
