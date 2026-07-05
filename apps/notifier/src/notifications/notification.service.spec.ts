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
