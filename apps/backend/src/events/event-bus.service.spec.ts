import { of, throwError } from 'rxjs';
import { EventBusService } from './event-bus.service';
import { EVENTS, RunEventPayload } from './event-bus.constants';

const payload: RunEventPayload = {
  event: EVENTS.RUN_COMPLETED,
  runId: 'r1',
  workflowId: 'w1',
  status: 'completed',
  step: 2,
  at: '2026-07-05T00:00:00.000Z',
};

describe('EventBusService.publish', () => {
  it('emits to RabbitMQ with the event name as pattern and to Kafka keyed by workflowId', async () => {
    const rabbit = { emit: jest.fn().mockReturnValue(of(undefined)) };
    const kafka = { emit: jest.fn().mockReturnValue(of(undefined)) };
    const bus = new EventBusService(rabbit as any, kafka as any);

    await bus.publish(payload);

    expect(rabbit.emit).toHaveBeenCalledWith('run.completed', payload);
    expect(kafka.emit).toHaveBeenCalledWith('run-events', {
      key: 'w1',
      value: payload,
    });
  });

  it('does not throw when a broker is down', async () => {
    const rabbit = {
      emit: jest.fn().mockReturnValue(throwError(() => new Error('ECONNREFUSED'))),
    };
    const kafka = { emit: jest.fn().mockReturnValue(of(undefined)) };
    const bus = new EventBusService(rabbit as any, kafka as any);

    await expect(bus.publish(payload)).resolves.toBeUndefined();
    expect(kafka.emit).toHaveBeenCalled();
  });
});
