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
