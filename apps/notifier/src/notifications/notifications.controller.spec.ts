import { NotificationsController } from './notifications.controller';

function ctx(routingKey: string, redelivered: boolean) {
  const message = { fields: { routingKey, redelivered } };
  const channel = { ack: jest.fn(), nack: jest.fn() };
  return {
    getChannelRef: () => channel,
    getMessage: () => message,
    _channel: channel,
    _message: message,
  } as any;
}

describe('NotificationsController.consume', () => {
  it('records and acks on success', async () => {
    const service = { record: jest.fn().mockResolvedValue(undefined) };
    const controller = new NotificationsController(service as any);
    const context = ctx('run.advanced', false);

    await controller.handleRunEvent({ event: 'run.advanced' }, context);

    expect(service.record).toHaveBeenCalledWith({ event: 'run.advanced' });
    expect(context._channel.ack).toHaveBeenCalledWith(context._message);
    expect(context._channel.nack).not.toHaveBeenCalled();
  });

  it('requeues once when processing throws on first delivery', async () => {
    const service = { record: jest.fn().mockRejectedValue(new Error('boom')) };
    const controller = new NotificationsController(service as any);
    const context = ctx('run.advanced', false);

    await controller.handleRunEvent({ event: 'run.advanced' }, context);

    expect(context._channel.nack).toHaveBeenCalledWith(context._message, false, true);
  });

  it('dead-letters when processing throws on a redelivered message', async () => {
    const service = { record: jest.fn().mockRejectedValue(new Error('boom')) };
    const controller = new NotificationsController(service as any);
    const context = ctx('run.advanced', true);

    await controller.handleRunEvent({ event: 'run.advanced' }, context);

    expect(context._channel.nack).toHaveBeenCalledWith(context._message, false, false);
  });
});
