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
