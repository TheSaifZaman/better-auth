import { of, throwError } from 'rxjs';
import { EventBusService } from './event-bus.service';
import { EVENTS, DomainEvent } from './event-bus.constants';

const runEvent: DomainEvent = {
  event: EVENTS.RUN_COMPLETED,
  runId: 'r1',
  workflowId: 'w1',
  status: 'completed',
  step: 2,
  at: '2026-07-05T00:00:00.000Z',
};

describe('EventBusService.publish', () => {
  it('emits to RabbitMQ (event name) and sends plain JSON to Kafka keyed by workflowId', async () => {
    const rabbit = { emit: jest.fn().mockReturnValue(of(undefined)) };
    const producer = { send: jest.fn().mockResolvedValue(undefined) };
    const bus = new EventBusService(rabbit as any, producer as any);

    await bus.publish(runEvent);

    expect(rabbit.emit).toHaveBeenCalledWith('run.completed', runEvent);
    expect(producer.send).toHaveBeenCalledWith({
      topic: 'run-events',
      messages: [{ key: 'w1', value: JSON.stringify(runEvent) }],
    });
  });

  it('keys auth events by userId when there is no workflowId', async () => {
    const authEvent: DomainEvent = {
      event: EVENTS.AUTH_USER_CREATED,
      userId: 'u1',
      at: '2026-07-05T00:00:00.000Z',
    };
    const rabbit = { emit: jest.fn().mockReturnValue(of(undefined)) };
    const producer = { send: jest.fn().mockResolvedValue(undefined) };
    const bus = new EventBusService(rabbit as any, producer as any);

    await bus.publish(authEvent);

    expect(producer.send).toHaveBeenCalledWith({
      topic: 'run-events',
      messages: [{ key: 'u1', value: JSON.stringify(authEvent) }],
    });
  });

  it('does not throw when a broker is down', async () => {
    const rabbit = { emit: jest.fn().mockReturnValue(throwError(() => new Error('ECONNREFUSED'))) };
    const producer = { send: jest.fn().mockRejectedValue(new Error('kafka down')) };
    const bus = new EventBusService(rabbit as any, producer as any);

    await expect(bus.publish(runEvent)).resolves.toBeUndefined();
  });
});
