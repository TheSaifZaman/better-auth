import { Inject, Injectable, NotFoundException } from '@nestjs/common';
import { eq } from 'drizzle-orm';
import { run, workflow } from '@repo/db';
import type { Database } from '@repo/db';
import { DATABASE_CONNECTION } from '../database/database-coonection';
import { EventBusService } from '../events/event-bus.service';
import { advanceStatus, RunStatus } from './run-state-machine';

@Injectable()
export class RunsService {
  constructor(
    @Inject(DATABASE_CONNECTION) private readonly db: Database,
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
