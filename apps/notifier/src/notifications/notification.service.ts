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
