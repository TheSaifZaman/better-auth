import { Inject, Injectable, Logger } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { firstValueFrom, Observable } from 'rxjs';
import {
  KAFKA_CLIENT,
  RABBITMQ_CLIENT,
  RunEventPayload,
} from './event-bus.constants';

@Injectable()
export class EventBusService {
  private readonly logger = new Logger(EventBusService.name);

  constructor(
    @Inject(RABBITMQ_CLIENT) private readonly rabbit: ClientProxy,
    @Inject(KAFKA_CLIENT) private readonly kafka: ClientProxy,
  ) {}

  // Fan the same event out to both brokers. A down broker is logged, not thrown:
  // publishing must never fail the HTTP request that triggered it.
  async publish(payload: RunEventPayload): Promise<void> {
    await Promise.all([
      this.emitTo('rabbitmq', () => this.rabbit.emit(payload.event, payload)),
      this.emitTo('kafka', () =>
        this.kafka.emit('run-events', { key: payload.workflowId, value: payload }),
      ),
    ]);
  }

  private async emitTo(
    name: string,
    emit: () => Observable<unknown>,
  ): Promise<void> {
    try {
      await firstValueFrom(emit());
    } catch (err) {
      this.logger.error(`failed to publish to ${name}: ${(err as Error).message}`);
    }
  }
}
