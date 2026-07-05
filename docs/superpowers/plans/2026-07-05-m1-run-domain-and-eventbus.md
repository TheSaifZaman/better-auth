# Milestone 1 — Run Domain & Dual-Publish EventBus — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a run/workflow domain to the NestJS backend where advancing a run publishes the same event to both RabbitMQ and Kafka via a single `EventBusService`.

**Architecture:** A pure state machine decides transitions and which events they emit. `RunsService` persists runs via Drizzle and calls `EventBusService.publish`, which fans the event out to a RabbitMQ topic exchange (routing key = event name) and a Kafka topic (message key = workflowId). Brokers are not required to run for this milestone — publish is fire-and-forget and unit tests mock the clients. This is the foundation the notifier (M2) and audit-go (M3) consume.

**Tech Stack:** NestJS 11, `@nestjs/microservices`, `kafkajs`, `amqp-connection-manager`/`amqplib`, Drizzle ORM (Postgres), Jest.

## Global Constraints

- NestJS `^11.0.1`; Node `>=18`; TypeScript `^5.7.3` (repo pins `5.9.2`).
- Package manager is **pnpm** (`pnpm@9.0.0`) in a Turborepo; run backend commands with `pnpm --filter backend <cmd>`.
- Drizzle config globs `./src/**/schema.ts` — a new `src/runs/schema.ts` is auto-discovered.
- All primary keys are native Postgres `uuid` with `defaultRandom()` (matches existing auth schema).
- Test files are `*.spec.ts` under `src/`, run via `pnpm --filter backend test` (ts-jest, `rootDir: src`).
- The DB connection token is the string `DATABASE_CONNECTION` from `src/database/database-coonection.ts` (note the existing filename typo — do not rename it).
- Run status values: `'queued' | 'running' | 'completed' | 'failed'`. Event names: `'run.advanced' | 'run.completed' | 'run.failed'`.

---

### Task 1: Messaging dependencies & event contract

**Files:**
- Modify: `apps/backend/package.json` (dependencies)
- Create: `apps/backend/src/events/event-bus.constants.ts`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `RABBITMQ_CLIENT: string`, `KAFKA_CLIENT: string` (DI tokens)
  - `EVENTS` const map and `EventName` union type
  - `interface RunEventPayload { event: EventName; runId: string; workflowId: string; status: string; step: number; at: string }`

- [ ] **Step 1: Install messaging dependencies**

Run:
```bash
cd /Users/nocturnal/Gitlab/better-auth
pnpm --filter backend add @nestjs/microservices@^11 kafkajs@^2 amqp-connection-manager@^4 amqplib@^0.10
pnpm --filter backend add -D @types/amqplib
```
Expected: `package.json` gains the four runtime deps + one dev dep; `pnpm-lock.yaml` updates.

- [ ] **Step 2: Create the event contract**

Create `apps/backend/src/events/event-bus.constants.ts`:
```ts
// DI tokens for the two producer clients.
export const RABBITMQ_CLIENT = 'RABBITMQ_CLIENT';
export const KAFKA_CLIENT = 'KAFKA_CLIENT';

// Logical event names. On RabbitMQ these are the topic-exchange routing keys.
export const EVENTS = {
  RUN_ADVANCED: 'run.advanced',
  RUN_COMPLETED: 'run.completed',
  RUN_FAILED: 'run.failed',
} as const;

export type EventName = (typeof EVENTS)[keyof typeof EVENTS];

// The single payload shape published to BOTH brokers.
export interface RunEventPayload {
  event: EventName;
  runId: string;
  workflowId: string;
  status: string;
  step: number;
  at: string; // ISO-8601 timestamp
}
```

- [ ] **Step 3: Verify it compiles**

Run: `pnpm --filter backend exec tsc --noEmit`
Expected: PASS (no errors).

- [ ] **Step 4: Commit**

```bash
git add apps/backend/package.json apps/backend/src/events/event-bus.constants.ts pnpm-lock.yaml
git commit -m "feat(backend): add messaging deps and event contract"
```

---

### Task 2: Run state machine (pure, TDD)

**Files:**
- Create: `apps/backend/src/runs/run-state-machine.ts`
- Test: `apps/backend/src/runs/run-state-machine.spec.ts`

**Interfaces:**
- Consumes: `EVENTS`, `EventName` from `../events/event-bus.constants`.
- Produces:
  - `type RunStatus = 'queued' | 'running' | 'completed' | 'failed'`
  - `interface Transition { next: RunStatus; events: EventName[] }`
  - `function advanceStatus(current: RunStatus): Transition`
  - `function failRun(): Transition`

- [ ] **Step 1: Write the failing test**

Create `apps/backend/src/runs/run-state-machine.spec.ts`:
```ts
import { advanceStatus, failRun } from './run-state-machine';
import { EVENTS } from '../events/event-bus.constants';

describe('advanceStatus', () => {
  it('queued -> running emits run.advanced', () => {
    expect(advanceStatus('queued')).toEqual({
      next: 'running',
      events: [EVENTS.RUN_ADVANCED],
    });
  });

  it('running -> completed emits run.advanced and run.completed', () => {
    expect(advanceStatus('running')).toEqual({
      next: 'completed',
      events: [EVENTS.RUN_ADVANCED, EVENTS.RUN_COMPLETED],
    });
  });

  it('throws when advancing a completed run', () => {
    expect(() => advanceStatus('completed')).toThrow('run already completed');
  });

  it('throws when advancing a failed run', () => {
    expect(() => advanceStatus('failed')).toThrow('run already failed');
  });
});

describe('failRun', () => {
  it('emits run.failed', () => {
    expect(failRun()).toEqual({ next: 'failed', events: [EVENTS.RUN_FAILED] });
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --filter backend test run-state-machine`
Expected: FAIL — cannot find module `./run-state-machine`.

- [ ] **Step 3: Write the implementation**

Create `apps/backend/src/runs/run-state-machine.ts`:
```ts
import { EVENTS, EventName } from '../events/event-bus.constants';

export type RunStatus = 'queued' | 'running' | 'completed' | 'failed';

export interface Transition {
  next: RunStatus;
  events: EventName[];
}

// Advancing steps a run forward one stage and reports which events to publish.
export function advanceStatus(current: RunStatus): Transition {
  switch (current) {
    case 'queued':
      return { next: 'running', events: [EVENTS.RUN_ADVANCED] };
    case 'running':
      return {
        next: 'completed',
        events: [EVENTS.RUN_ADVANCED, EVENTS.RUN_COMPLETED],
      };
    case 'completed':
      throw new Error('run already completed');
    case 'failed':
      throw new Error('run already failed');
  }
}

export function failRun(): Transition {
  return { next: 'failed', events: [EVENTS.RUN_FAILED] };
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pnpm --filter backend test run-state-machine`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add apps/backend/src/runs/run-state-machine.ts apps/backend/src/runs/run-state-machine.spec.ts
git commit -m "feat(backend): add run state machine"
```

---

### Task 3: Drizzle schema for workflow & run

**Files:**
- Create: `apps/backend/src/runs/schema.ts`
- Modify: `apps/backend/src/database/database.module.ts`

**Interfaces:**
- Consumes: nothing.
- Produces: `workflow` and `run` Drizzle table objects (exported), registered in the shared Drizzle schema.

- [ ] **Step 1: Create the schema**

Create `apps/backend/src/runs/schema.ts`:
```ts
import { relations } from 'drizzle-orm';
import { pgTable, uuid, text, integer, timestamp, index } from 'drizzle-orm/pg-core';

export const workflow = pgTable('workflow', {
  id: uuid('id').primaryKey().defaultRandom(),
  name: text('name').notNull(),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});

export const run = pgTable(
  'run',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    workflowId: uuid('workflow_id')
      .notNull()
      .references(() => workflow.id, { onDelete: 'cascade' }),
    // One of: queued | running | completed | failed
    status: text('status').notNull().default('queued'),
    step: integer('step').notNull().default(0),
    createdAt: timestamp('created_at').defaultNow().notNull(),
    updatedAt: timestamp('updated_at')
      .defaultNow()
      .$onUpdate(() => /* @__PURE__ */ new Date())
      .notNull(),
  },
  (table) => [index('run_workflowId_idx').on(table.workflowId)],
);

export const workflowRelations = relations(workflow, ({ many }) => ({
  runs: many(run),
}));

export const runRelations = relations(run, ({ one }) => ({
  workflow: one(workflow, {
    fields: [run.workflowId],
    references: [workflow.id],
  }),
}));
```

- [ ] **Step 2: Register the schema in the Drizzle connection**

In `apps/backend/src/database/database.module.ts`, add the import beside the existing auth-schema import:
```ts
import * as authSchema from '../auth/schema';
import * as runsSchema from '../runs/schema';
```
and spread it into the `drizzle(pool, { schema: { ... } })` call:
```ts
        return drizzle(pool, {
          schema: {
            ...authSchema,
            ...runsSchema,
          },
        });
```

- [ ] **Step 3: Generate the migration**

Run: `pnpm --filter backend exec drizzle-kit generate`
Expected: a new SQL file under `apps/backend/drizzle/` creating tables `workflow` and `run` (drizzle-kit reads `./src/**/schema.ts`; no DB connection needed for `generate`).

- [ ] **Step 4: Verify it compiles**

Run: `pnpm --filter backend exec tsc --noEmit`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/backend/src/runs/schema.ts apps/backend/src/database/database.module.ts apps/backend/drizzle
git commit -m "feat(backend): add workflow and run tables"
```

---

### Task 4: EventBusService fan-out (TDD, mocked clients)

**Files:**
- Create: `apps/backend/src/events/event-bus.service.ts`
- Test: `apps/backend/src/events/event-bus.service.spec.ts`

**Interfaces:**
- Consumes: `RABBITMQ_CLIENT`, `KAFKA_CLIENT`, `RunEventPayload` from `./event-bus.constants`; `ClientProxy` from `@nestjs/microservices`.
- Produces: `class EventBusService { publish(payload: RunEventPayload): Promise<void> }`.

- [ ] **Step 1: Write the failing test**

Create `apps/backend/src/events/event-bus.service.spec.ts`:
```ts
import { of, throwError } from 'rxjs';
import { EventBusService } from './event-bus.service';
import { EVENTS, RunEventPayload } from './event-bus.constants';

const payload: RunEventPayload = {
  event: EVENTS.RUN_COMPLETED,
  runId: 'r1',
  workflowId: 'w1',
  status: 'completed',
  step: 2,
  at: '2026-07-05T00:00:00.000Z',
};

describe('EventBusService.publish', () => {
  it('emits to RabbitMQ with the event name as pattern and to Kafka keyed by workflowId', async () => {
    const rabbit = { emit: jest.fn().mockReturnValue(of(undefined)) };
    const kafka = { emit: jest.fn().mockReturnValue(of(undefined)) };
    const bus = new EventBusService(rabbit as any, kafka as any);

    await bus.publish(payload);

    expect(rabbit.emit).toHaveBeenCalledWith('run.completed', payload);
    expect(kafka.emit).toHaveBeenCalledWith('run-events', {
      key: 'w1',
      value: payload,
    });
  });

  it('does not throw when a broker is down', async () => {
    const rabbit = { emit: jest.fn().mockReturnValue(throwError(() => new Error('ECONNREFUSED'))) };
    const kafka = { emit: jest.fn().mockReturnValue(of(undefined)) };
    const bus = new EventBusService(rabbit as any, kafka as any);

    await expect(bus.publish(payload)).resolves.toBeUndefined();
    expect(kafka.emit).toHaveBeenCalled();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --filter backend test event-bus.service`
Expected: FAIL — cannot find module `./event-bus.service`.

- [ ] **Step 3: Write the implementation**

Create `apps/backend/src/events/event-bus.service.ts`:
```ts
import { Inject, Injectable, Logger } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import {
  KAFKA_CLIENT,
  RABBITMQ_CLIENT,
  RunEventPayload,
} from './event-bus.constants';

@Injectable()
export class EventBusService {
  private readonly logger = new Logger(EventBusService.name);

  constructor(
    @Inject(RABBITMQ_CLIENT) private readonly rabbit: ClientProxy,
    @Inject(KAFKA_CLIENT) private readonly kafka: ClientProxy,
  ) {}

  // Fan the same event out to both brokers. A down broker is logged, not thrown:
  // publishing must never fail the HTTP request that triggered it.
  async publish(payload: RunEventPayload): Promise<void> {
    await Promise.all([
      this.emitTo('rabbitmq', () => this.rabbit.emit(payload.event, payload)),
      this.emitTo('kafka', () =>
        this.kafka.emit('run-events', { key: payload.workflowId, value: payload }),
      ),
    ]);
  }

  private async emitTo(
    name: string,
    emit: () => import('rxjs').Observable<unknown>,
  ): Promise<void> {
    try {
      await firstValueFrom(emit());
    } catch (err) {
      this.logger.error(`failed to publish to ${name}: ${(err as Error).message}`);
    }
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pnpm --filter backend test event-bus.service`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add apps/backend/src/events/event-bus.service.ts apps/backend/src/events/event-bus.service.spec.ts
git commit -m "feat(backend): add dual-publish EventBusService"
```

---

### Task 5: EventsModule (client registration)

**Files:**
- Create: `apps/backend/src/events/events.module.ts`
- Test: `apps/backend/src/events/events.module.spec.ts`

**Interfaces:**
- Consumes: `EventBusService`, client tokens.
- Produces: `@Global()` `EventsModule` that registers the two `ClientProxy` instances and exports `EventBusService`.

- [ ] **Step 1: Write the failing test**

Create `apps/backend/src/events/events.module.spec.ts`:
```ts
import { Test } from '@nestjs/testing';
import { EventsModule } from './events.module';
import { EventBusService } from './event-bus.service';

describe('EventsModule', () => {
  it('resolves EventBusService with both clients injected', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [EventsModule],
    }).compile();

    const bus = moduleRef.get(EventBusService);
    expect(bus).toBeInstanceOf(EventBusService);
    await moduleRef.close();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --filter backend test events.module`
Expected: FAIL — cannot find module `./events.module`.

- [ ] **Step 3: Write the implementation**

Create `apps/backend/src/events/events.module.ts`:
```ts
import { Global, Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { KAFKA_CLIENT, RABBITMQ_CLIENT } from './event-bus.constants';
import { EventBusService } from './event-bus.service';

// Clients connect lazily (on first emit), so registering them here does not
// require the brokers to be running — important for M1 where only the
// producer exists.
@Global()
@Module({
  imports: [
    ClientsModule.register([
      {
        name: RABBITMQ_CLIENT,
        transport: Transport.RMQ,
        options: {
          urls: [process.env.RABBITMQ_URL ?? 'amqp://localhost:5672'],
          // Publish to a topic exchange; emit(pattern) uses pattern as the
          // routing key. Consumers (M2) bind their own queues to this exchange.
          exchange: 'events',
          exchangeType: 'topic',
          wildcards: true,
          // Producer-only client: it never consumes, so no work queue.
          queue: 'backend-producer-unused',
        },
      },
      {
        name: KAFKA_CLIENT,
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'backend-producer',
            brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
          },
          producerOnlyMode: true,
        },
      },
    ]),
  ],
  providers: [EventBusService],
  exports: [EventBusService],
})
export class EventsModule {}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pnpm --filter backend test events.module`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/backend/src/events/events.module.ts apps/backend/src/events/events.module.spec.ts
git commit -m "feat(backend): register RabbitMQ and Kafka producer clients"
```

---

### Task 6: RunsService (persist + emit, TDD)

**Files:**
- Create: `apps/backend/src/runs/runs.service.ts`
- Test: `apps/backend/src/runs/runs.service.spec.ts`

**Interfaces:**
- Consumes: `DATABASE_CONNECTION` token, `EventBusService`, `advanceStatus`, `RunStatus`, `run`/`workflow` tables.
- Produces: `class RunsService { createRun(workflowName: string): Promise<Run>; advance(runId: string): Promise<Run> }` where `Run` is the row type inferred from the `run` table.

- [ ] **Step 1: Write the failing test**

Create `apps/backend/src/runs/runs.service.spec.ts`:
```ts
import { RunsService } from './runs.service';
import { EVENTS } from '../events/event-bus.constants';

// Minimal fakes for the two Drizzle query chains RunsService uses.
function selectDb(rows: any[]) {
  return { from: () => ({ where: () => Promise.resolve(rows) }) };
}
function updateDb(rows: any[]) {
  return { set: () => ({ where: () => ({ returning: () => Promise.resolve(rows) }) }) };
}

describe('RunsService.advance', () => {
  it('emits run.advanced then run.completed for a running run and persists the next state', async () => {
    const current = { id: 'r1', workflowId: 'w1', status: 'running', step: 1 };
    const updated = { id: 'r1', workflowId: 'w1', status: 'completed', step: 2 };
    const db = { select: () => selectDb([current]), update: () => updateDb([updated]) };
    const events = { publish: jest.fn().mockResolvedValue(undefined) };
    const service = new RunsService(db as any, events as any);

    const result = await service.advance('r1');

    expect(result).toEqual(updated);
    expect(events.publish.mock.calls.map((c) => c[0].event)).toEqual([
      EVENTS.RUN_ADVANCED,
      EVENTS.RUN_COMPLETED,
    ]);
    expect(events.publish).toHaveBeenLastCalledWith(
      expect.objectContaining({ runId: 'r1', workflowId: 'w1', status: 'completed', step: 2 }),
    );
  });

  it('throws NotFound when the run is missing', async () => {
    const db = { select: () => selectDb([]) };
    const events = { publish: jest.fn() };
    const service = new RunsService(db as any, events as any);

    await expect(service.advance('missing')).rejects.toThrow('run missing not found');
    expect(events.publish).not.toHaveBeenCalled();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --filter backend test runs.service`
Expected: FAIL — cannot find module `./runs.service`.

- [ ] **Step 3: Write the implementation**

Create `apps/backend/src/runs/runs.service.ts`:
```ts
import { Inject, Injectable, NotFoundException } from '@nestjs/common';
import { eq } from 'drizzle-orm';
import { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { DATABASE_CONNECTION } from '../database/database-coonection';
import { EventBusService } from '../events/event-bus.service';
import { advanceStatus, RunStatus } from './run-state-machine';
import { run, workflow } from './schema';

@Injectable()
export class RunsService {
  constructor(
    @Inject(DATABASE_CONNECTION) private readonly db: NodePgDatabase<any>,
    private readonly events: EventBusService,
  ) {}

  async createRun(workflowName: string) {
    const [wf] = await this.db
      .insert(workflow)
      .values({ name: workflowName })
      .returning();
    const [created] = await this.db
      .insert(run)
      .values({ workflowId: wf.id })
      .returning();
    return created;
  }

  async advance(runId: string) {
    const [current] = await this.db.select().from(run).where(eq(run.id, runId));
    if (!current) {
      throw new NotFoundException(`run ${runId} not found`);
    }

    const { next, events } = advanceStatus(current.status as RunStatus);

    const [updated] = await this.db
      .update(run)
      .set({ status: next, step: current.step + 1 })
      .where(eq(run.id, runId))
      .returning();

    for (const event of events) {
      await this.events.publish({
        event,
        runId: updated.id,
        workflowId: updated.workflowId,
        status: updated.status,
        step: updated.step,
        at: new Date().toISOString(),
      });
    }

    return updated;
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pnpm --filter backend test runs.service`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add apps/backend/src/runs/runs.service.ts apps/backend/src/runs/runs.service.spec.ts
git commit -m "feat(backend): add RunsService that persists and emits"
```

---

### Task 7: RunsController, RunsModule & app wiring

**Files:**
- Create: `apps/backend/src/runs/runs.controller.ts`
- Create: `apps/backend/src/runs/runs.module.ts`
- Test: `apps/backend/src/runs/runs.controller.spec.ts`
- Modify: `apps/backend/src/app.module.ts`

**Interfaces:**
- Consumes: `RunsService`, `EventsModule`, `DatabaseModule`, `@Public` from `@thallesp/nestjs-better-auth`.
- Produces: `POST /runs` (body `{ workflow?: string }`) and `POST /runs/:id/advance`, both public; `RunsModule` wired into `AppModule`.

- [ ] **Step 1: Write the failing test**

Create `apps/backend/src/runs/runs.controller.spec.ts`:
```ts
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import * as request from 'supertest';
import { RunsController } from './runs.controller';
import { RunsService } from './runs.service';

describe('RunsController', () => {
  let app: INestApplication;
  const service = {
    createRun: jest.fn().mockResolvedValue({ id: 'r1', workflowId: 'w1', status: 'queued', step: 0 }),
    advance: jest.fn().mockResolvedValue({ id: 'r1', workflowId: 'w1', status: 'running', step: 1 }),
  };

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      controllers: [RunsController],
      providers: [{ provide: RunsService, useValue: service }],
    }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
  });

  afterAll(async () => await app.close());

  it('POST /runs creates a run', async () => {
    await request(app.getHttpServer())
      .post('/runs')
      .send({ workflow: 'demo' })
      .expect(201);
    expect(service.createRun).toHaveBeenCalledWith('demo');
  });

  it('POST /runs/:id/advance advances a run', async () => {
    await request(app.getHttpServer())
      .post('/runs/r1/advance')
      .expect(201);
    expect(service.advance).toHaveBeenCalledWith('r1');
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --filter backend test runs.controller`
Expected: FAIL — cannot find module `./runs.controller`.

- [ ] **Step 3: Write the controller**

Create `apps/backend/src/runs/runs.controller.ts`:
```ts
import { Body, Controller, Param, Post } from '@nestjs/common';
import { Public } from '@thallesp/nestjs-better-auth';
import { RunsService } from './runs.service';

// Public for the learning exercise — no session required to drive runs.
@Public()
@Controller('runs')
export class RunsController {
  constructor(private readonly runs: RunsService) {}

  @Post()
  create(@Body('workflow') workflowName?: string) {
    return this.runs.createRun(workflowName ?? 'default');
  }

  @Post(':id/advance')
  advance(@Param('id') id: string) {
    return this.runs.advance(id);
  }
}
```

- [ ] **Step 4: Write the module**

Create `apps/backend/src/runs/runs.module.ts`:
```ts
import { Module } from '@nestjs/common';
import { DatabaseModule } from '../database/database.module';
import { RunsController } from './runs.controller';
import { RunsService } from './runs.service';

@Module({
  imports: [DatabaseModule],
  controllers: [RunsController],
  providers: [RunsService],
})
export class RunsModule {}
```

- [ ] **Step 5: Wire modules into AppModule**

In `apps/backend/src/app.module.ts`, add imports at the top:
```ts
import { RunsModule } from './runs/runs.module';
import { EventsModule } from './events/events.module';
```
and add both to the `imports` array (alongside `UsersModule`):
```ts
  imports: [
    UsersModule,
    EventsModule,
    RunsModule,
    ConfigModule.forRoot(),
    // ...existing AuthModule.forRootAsync(...) unchanged
```

- [ ] **Step 6: Run the controller test to verify it passes**

Run: `pnpm --filter backend test runs.controller`
Expected: PASS (2 tests).

- [ ] **Step 7: Run the full suite and typecheck**

Run: `pnpm --filter backend test && pnpm --filter backend exec tsc --noEmit`
Expected: all suites PASS; tsc reports no errors.

- [ ] **Step 8: Commit**

```bash
git add apps/backend/src/runs/runs.controller.ts apps/backend/src/runs/runs.module.ts apps/backend/src/runs/runs.controller.spec.ts apps/backend/src/app.module.ts
git commit -m "feat(backend): expose runs endpoints and wire modules"
```

---

## Definition of done (Milestone 1)

- `pnpm --filter backend test` passes (state machine, event bus, events module, runs service, runs controller).
- `pnpm --filter backend exec tsc --noEmit` is clean.
- A migration for `workflow` + `run` exists under `apps/backend/drizzle/`.
- `EventBusService.publish` fans out to both clients and swallows broker-down errors.
- Endpoints `POST /runs` and `POST /runs/:id/advance` exist and are public.

Brokers are intentionally NOT exercised here — that begins in Milestone 2 (RabbitMQ notifier) and Milestone 3 (Go audit-go), each of which gets its own plan.

## Deferred to later milestones (not in this plan)

- **M2:** `apps/notifier` NestJS RMQ microservice — exchange/queue/DLQ, manual ack, `notification` rows.
- **M3:** `apps/audit-go` Kafka consumer — `audit_log`, live count map + snapshot (`workflow_run_counts`), one Go lib first.
- **M4:** second Go lib behind the interface, `--replay` mode, auth-event producer (better-auth `@Hook`), `docker-compose.yml`, `scripts/demo.sh`.
