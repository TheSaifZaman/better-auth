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
