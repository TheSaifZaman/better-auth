import {
  pgTable,
  bigserial,
  bigint,
  integer,
  text,
  jsonb,
  timestamp,
} from 'drizzle-orm/pg-core';

// Append-only log written by the Go audit service. Decoupled from the
// producer tables: workflow_id / run_id are plain text with no FK, because a
// consumer across a broker boundary should not share referential integrity
// with the producer.
export const auditLog = pgTable('audit_log', {
  id: bigserial('id', { mode: 'number' }).primaryKey(),
  eventType: text('event_type').notNull(),
  workflowId: text('workflow_id'),
  runId: text('run_id'),
  payload: jsonb('payload').notNull(),
  kafkaPartition: integer('kafka_partition').notNull(),
  kafkaOffset: bigint('kafka_offset', { mode: 'number' }).notNull(),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});

// Live per-workflow completed-run count, snapshotted by the audit service.
export const workflowRunCounts = pgTable('workflow_run_counts', {
  workflowId: text('workflow_id').primaryKey(),
  completedCount: integer('completed_count').notNull().default(0),
  updatedAt: timestamp('updated_at')
    .defaultNow()
    .$onUpdate(() => /* @__PURE__ */ new Date())
    .notNull(),
});
