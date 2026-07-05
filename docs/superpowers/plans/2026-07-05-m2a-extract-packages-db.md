# Milestone 2a — Extract `@repo/db` Shared Package — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the Drizzle schema and DB connection into a shared `@repo/db` workspace package (adding the `notification` table) so the backend and the upcoming notifier app share one canonical schema.

**Architecture:** A new `packages/db` internal package holds all Drizzle table definitions, a barrel export, and a `createDatabase(url)` factory. Because the NestJS backend runs **compiled JS** (it does not transpile workspace TS the way Next.js does), the package emits a `dist/` build consumed at runtime; jest maps `@repo/db` to the TypeScript source so unit tests need no build. The backend is repointed from its local schema files to `@repo/db`, and migration ownership moves into the package.

**Tech Stack:** Turborepo internal package (`@repo/*` convention), Drizzle ORM + drizzle-kit, TypeScript (NodeNext, CJS output), Jest/ts-jest.

## Global Constraints

- pnpm workspace (`pnpm@9.0.0`); packages live under `packages/*` (already in `pnpm-workspace.yaml`).
- Internal packages use the `@repo/` scope and extend `@repo/typescript-config/base.json`.
- `@repo/db` must emit a CJS `dist/` build (`main`/`types` point at `dist`) — the backend requires it at runtime. It is NOT a source-only package.
- Backend runtime/build resolves `@repo/db` from `dist`; backend **jest** resolves it from source via `moduleNameMapper`.
- Do NOT rename the existing DI token file `src/database/database-coonection.ts` (typo is load-bearing).
- Table export names must stay identical (`user`, `session`, `account`, `verification`, `twoFactor`, `workflow`, `run`) — better-auth's drizzle adapter resolves tables by these names.
- After this milestone the backend's existing 12 unit tests must still pass and `nest build` must still succeed.

---

### Task 1: Scaffold the `@repo/db` package

**Files:**
- Create: `packages/db/package.json`
- Create: `packages/db/tsconfig.json`
- Create: `packages/db/src/index.ts` (temporary placeholder)
- Modify: `apps/backend/package.json` (add dependency)

**Interfaces:**
- Consumes: `@repo/typescript-config/base.json`.
- Produces: an installable `@repo/db` package whose `build` script emits `dist/index.js` + `dist/index.d.ts`.

- [ ] **Step 1: Create the package manifest**

Create `packages/db/package.json`:
```json
{
  "name": "@repo/db",
  "version": "0.0.0",
  "private": true,
  "type": "commonjs",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "default": "./dist/index.js"
    }
  },
  "scripts": {
    "build": "tsc",
    "check-types": "tsc --noEmit",
    "db:generate": "drizzle-kit generate",
    "db:push": "drizzle-kit push"
  },
  "dependencies": {
    "drizzle-orm": "^0.45.2",
    "pg": "^8.22.0"
  },
  "devDependencies": {
    "@repo/typescript-config": "workspace:*",
    "@types/pg": "^8.20.0",
    "drizzle-kit": "^0.31.10",
    "typescript": "5.9.2"
  }
}
```

- [ ] **Step 2: Create the package tsconfig**

Create `packages/db/tsconfig.json`:
```json
{
  "extends": "@repo/typescript-config/base.json",
  "compilerOptions": {
    "outDir": "./dist",
    "rootDir": "./src"
  },
  "include": ["src"],
  "exclude": ["node_modules", "dist"]
}
```

- [ ] **Step 3: Create a temporary placeholder entry**

Create `packages/db/src/index.ts`:
```ts
// Populated in Task 2.
export const __db_package = true;
```

- [ ] **Step 4: Add the dependency to the backend**

In `apps/backend/package.json`, add to `dependencies` (alphabetically near the top of the block):
```json
    "@repo/db": "workspace:*",
```

- [ ] **Step 5: Install and build**

Run:
```bash
cd /Users/nocturnal/Gitlab/better-auth
pnpm install
pnpm --filter @repo/db build
```
Expected: `pnpm install` links `@repo/db` into `apps/backend/node_modules`; the build emits `packages/db/dist/index.js` and `packages/db/dist/index.d.ts`.

- [ ] **Step 6: Commit**

```bash
git add packages/db/package.json packages/db/tsconfig.json packages/db/src/index.ts apps/backend/package.json pnpm-lock.yaml
git commit -m "chore(db): scaffold @repo/db package"
```

---

### Task 2: Move schema into the package and add the notification table

**Files:**
- Create: `packages/db/src/schema/auth.ts` (moved from `apps/backend/src/auth/schema.ts`)
- Create: `packages/db/src/schema/runs.ts` (moved from `apps/backend/src/runs/schema.ts`)
- Create: `packages/db/src/schema/notifications.ts` (new)
- Create: `packages/db/src/schema/index.ts`
- Create: `packages/db/src/client.ts`
- Modify: `packages/db/src/index.ts`

**Interfaces:**
- Consumes: nothing outside the package.
- Produces (all re-exported from `@repo/db`):
  - Auth tables: `user`, `session`, `account`, `verification`, `twoFactor` (+ their `*Relations`)
  - Run tables: `workflow`, `run` (+ `workflowRelations`, `runRelations`)
  - `notification` table (+ `notificationRelations`)
  - `type Database = NodePgDatabase<typeof schema>`
  - `function createDatabase(connectionString: string): Database`

- [ ] **Step 1: Move the auth schema verbatim**

Create `packages/db/src/schema/auth.ts` with the exact contents of the current `apps/backend/src/auth/schema.ts` (all five tables and their relations — `user`, `twoFactor`, `session`, `account`, `verification`, and `userRelations`, `twoFactorRelations`, `sessionRelations`, `accountRelations`). Copy it byte-for-byte; the import line `import { relations } from 'drizzle-orm'` and `import { pgTable, ... } from 'drizzle-orm/pg-core'` stay as-is.

- [ ] **Step 2: Move the runs schema verbatim**

Create `packages/db/src/schema/runs.ts` with the exact contents of the current `apps/backend/src/runs/schema.ts` (`workflow`, `run`, `workflowRelations`, `runRelations`).

- [ ] **Step 3: Add the notification table**

Create `packages/db/src/schema/notifications.ts`:
```ts
import { relations } from 'drizzle-orm';
import { pgTable, uuid, text, jsonb, timestamp, index } from 'drizzle-orm/pg-core';
import { run } from './runs';

// Written by the notifier (M2b) after it consumes an event from RabbitMQ.
// runId is nullable: auth events (M4) carry no run.
export const notification = pgTable(
  'notification',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    runId: uuid('run_id').references(() => run.id, { onDelete: 'set null' }),
    type: text('type').notNull(),
    payload: jsonb('payload').notNull(),
    createdAt: timestamp('created_at').defaultNow().notNull(),
  },
  (table) => [index('notification_runId_idx').on(table.runId)],
);

export const notificationRelations = relations(notification, ({ one }) => ({
  run: one(run, {
    fields: [notification.runId],
    references: [run.id],
  }),
}));
```

- [ ] **Step 4: Create the schema barrel**

Create `packages/db/src/schema/index.ts`:
```ts
export * from './auth';
export * from './runs';
export * from './notifications';
```

- [ ] **Step 5: Create the connection factory**

Create `packages/db/src/client.ts`:
```ts
import { drizzle, NodePgDatabase } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';
import * as schema from './schema';

export type Database = NodePgDatabase<typeof schema>;

// Single place that builds a Drizzle connection over the whole schema.
export function createDatabase(connectionString: string): Database {
  const pool = new Pool({ connectionString });
  return drizzle(pool, { schema });
}
```

- [ ] **Step 6: Replace the package entry**

Replace the entire contents of `packages/db/src/index.ts` with:
```ts
export * from './schema';
export * from './client';
```

- [ ] **Step 7: Build the package**

Run: `pnpm --filter @repo/db build`
Expected: PASS; `packages/db/dist/` contains `index.js`, `client.js`, `schema/*.js` and matching `.d.ts` files.

- [ ] **Step 8: Commit**

```bash
git add packages/db/src
git commit -m "feat(db): move schema into @repo/db and add notification table"
```

---

### Task 3: Repoint the backend to `@repo/db`

**Files:**
- Modify: `apps/backend/src/database/database.module.ts`
- Modify: `apps/backend/src/runs/runs.service.ts`
- Modify: `apps/backend/package.json` (jest `moduleNameMapper`)
- Delete: `apps/backend/src/auth/schema.ts`
- Delete: `apps/backend/src/runs/schema.ts`

**Interfaces:**
- Consumes: `createDatabase`, `Database`, `run`, `workflow` from `@repo/db`.
- Produces: a backend that imports all schema from `@repo/db`; no local schema files remain.

- [ ] **Step 1: Repoint the database module**

Replace the entire contents of `apps/backend/src/database/database.module.ts` with:
```ts
import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { createDatabase } from '@repo/db';
import { DATABASE_CONNECTION } from './database-coonection';

@Module({
  imports: [ConfigModule],
  providers: [
    {
      provide: DATABASE_CONNECTION,
      useFactory: (configService: ConfigService) =>
        createDatabase(configService.getOrThrow('DATABASE_URL')),
      inject: [ConfigService],
    },
  ],
  exports: [DATABASE_CONNECTION],
})
export class DatabaseModule {}
```

- [ ] **Step 2: Repoint RunsService imports**

In `apps/backend/src/runs/runs.service.ts`, change the two import lines:
```ts
import { NodePgDatabase } from 'drizzle-orm/node-postgres';
```
```ts
import { run, workflow } from './schema';
```
to:
```ts
import { Database, run, workflow } from '@repo/db';
```
and change the constructor field type from `NodePgDatabase<any>` to `Database`:
```ts
    @Inject(DATABASE_CONNECTION) private readonly db: Database,
```
(The `import { eq } from 'drizzle-orm'` line stays unchanged.)

- [ ] **Step 3: Add the jest module mapping**

In `apps/backend/package.json`, inside the `"jest"` object, add a `moduleNameMapper` key (after `"transform"`):
```json
    "moduleNameMapper": {
      "^@repo/db$": "<rootDir>/../../../packages/db/src/index.ts"
    },
```
This makes ts-jest compile the package from source during tests (the `dist` build is only for runtime).

- [ ] **Step 4: Delete the old schema files**

Run:
```bash
git rm apps/backend/src/auth/schema.ts apps/backend/src/runs/schema.ts
```

- [ ] **Step 5: Run the backend unit tests**

Run: `pnpm --filter backend test`
Expected: 5 suites, 12 tests PASS (the `EventBusService` ECONNREFUSED log line is expected).

- [ ] **Step 6: Typecheck and build**

Run: `pnpm --filter backend exec tsc --noEmit && pnpm --filter backend build`
Expected: both clean; `apps/backend/dist/main.js` present.

- [ ] **Step 7: Commit**

```bash
git add apps/backend/src/database/database.module.ts apps/backend/src/runs/runs.service.ts apps/backend/package.json
git commit -m "refactor(backend): consume schema from @repo/db"
```

---

### Task 4: Move migration ownership into the package

**Files:**
- Create: `packages/db/drizzle.config.ts`
- Move: `apps/backend/drizzle/` → `packages/db/drizzle/`
- Delete: `apps/backend/drizzle.config.ts`

**Interfaces:**
- Consumes: `packages/db/src/schema/index.ts` (drizzle-kit reads it).
- Produces: a `notification` migration under `packages/db/drizzle/`, with the package owning all future migrations.

- [ ] **Step 1: Create the package drizzle config**

Create `packages/db/drizzle.config.ts`:
```ts
import { defineConfig } from 'drizzle-kit';

export default defineConfig({
  schema: './src/schema/index.ts',
  out: './drizzle',
  dialect: 'postgresql',
  dbCredentials: {
    url: process.env.DATABASE_URL!,
  },
});
```

- [ ] **Step 2: Move existing migrations and delete the old config**

Run:
```bash
cd /Users/nocturnal/Gitlab/better-auth
git mv apps/backend/drizzle packages/db/drizzle
git rm apps/backend/drizzle.config.ts
```
Expected: `packages/db/drizzle/` now holds `0000_orange_nebula.sql`, `0001_eager_crystal.sql`, and `meta/` (with `_journal.json`).

- [ ] **Step 3: Generate the notification migration**

Run: `pnpm --filter @repo/db exec drizzle-kit generate`
Expected: a new `packages/db/drizzle/0002_*.sql` that `CREATE TABLE "notification"` (and the FK to `run` + index); drizzle-kit reports 8 tables and only the notification table as new.

- [ ] **Step 4: Verify the migration content**

Run: `cat packages/db/drizzle/0002_*.sql`
Expected: contains `CREATE TABLE "notification"` with columns `id`, `run_id`, `type`, `payload`, `created_at`, a FK `run_id → run(id) ON DELETE set null`, and `notification_runId_idx`.

- [ ] **Step 5: Full verification pass**

Run:
```bash
pnpm --filter @repo/db build && pnpm --filter backend test && pnpm --filter backend build
```
Expected: package builds; 12 backend tests pass; backend builds.

- [ ] **Step 6: Commit**

```bash
git add packages/db/drizzle.config.ts packages/db/drizzle
git commit -m "chore(db): move migrations into @repo/db and add notification table"
```

---

## Definition of done (Milestone 2a)

- `@repo/db` exports all schema tables + `createDatabase`/`Database`, builds to `dist/`.
- Backend imports schema exclusively from `@repo/db`; no `src/**/schema.ts` remain in the backend.
- Backend's 12 unit tests pass; `tsc --noEmit` and `nest build` are clean.
- A `notification` migration exists under `packages/db/drizzle/`.

## Note on running the app

Because `@repo/db` is consumed as a built package, run `pnpm --filter @repo/db build` (or `turbo build`) before `pnpm --filter backend dev`/`start`. This only matters once M2b runs the app against real infra; M2a is verified by tests + build alone.

## Next (Plan 2b, separate)

`apps/notifier` — a NestJS RMQ microservice: bind queue `notifications` to exchange `events` (`run.#`, `auth.#`), manual ack, **limited requeue → DLQ** (`notifications.dlq` via `events.dlx`, bounded by redelivery count), writing `notification` rows through `@repo/db`.
