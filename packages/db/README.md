# @repo/db

Shared **Drizzle schema, connection factory, and migrations** for the [better-auth monorepo](../../README.md). Both `apps/backend` and `apps/notifier` consume it; the Go service (`apps/audit-go`) reads the same tables directly.

Because the NestJS apps run **compiled JS** (unlike Next.js, which transpiles workspace TS), this package emits a real `dist/` build — build it before running the backend/notifier.

## Contents

- `src/schema/` — `auth` (user/session/account/verification/two_factor), `runs` (workflow/run), `notifications` (notification), `audit` (audit_log/workflow_run_counts)
- `src/client.ts` — `createDatabase(url)` + the `Database` type
- `drizzle/` — generated SQL migrations (this package owns them)

## Usage

```sh
pnpm --filter @repo/db build                       # emit dist/ (required before backend/notifier run)
pnpm --filter @repo/db exec drizzle-kit generate   # create a migration after editing schema
DATABASE_URL=postgres://.../db pnpm --filter @repo/db exec drizzle-kit migrate   # apply migrations
```

```ts
import { createDatabase, run, workflow, notification } from '@repo/db';
const db = createDatabase(process.env.DATABASE_URL!);
```

> Ownership note: `audit_log` / `workflow_run_counts` are `text` workflow/run ids with **no FK** — the Go consumer is decoupled from the producer tables across the broker boundary.
