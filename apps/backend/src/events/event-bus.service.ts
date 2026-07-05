import {
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import type { Producer } from 'kafkajs';
import {
  DomainEvent,
  KAFKA_PRODUCER,
  RABBITMQ_CLIENT,
} from './event-bus.constants';

@Injectable()
export class EventBusService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(EventBusService.name);

  constructor(
    @Inject(RABBITMQ_CLIENT) private readonly rabbit: ClientProxy,
    @Inject(KAFKA_PRODUCER) private readonly kafka: Producer,
  ) {}

  async onModuleInit(): Promise<void> {
    try {
      await this.kafka.connect();
    } catch (err) {
      // Best effort: send() will reconnect on demand if the broker returns.
      this.logger.warn(`kafka producer connect deferred: ${(err as Error).message}`);
    }
  }

  async onModuleDestroy(): Promise<void> {
    try {
      await this.kafka.disconnect();
    } catch {
      // ignore shutdown errors
    }
  }

  // Fan the event to both brokers. Neither failure propagates to the caller.
  async publish(event: DomainEvent): Promise<void> {
    await Promise.all([this.toRabbit(event), this.toKafka(event)]);
  }

  private async toRabbit(event: DomainEvent): Promise<void> {
    try {
      await firstValueFrom(this.rabbit.emit(event.event, event));
    } catch (err) {
      this.logger.error(`rabbitmq publish failed: ${(err as Error).message}`);
    }
  }

  private async toKafka(event: DomainEvent): Promise<void> {
    try {
      await this.kafka.send({
        topic: 'run-events',
        messages: [{ key: event.workflowId ?? event.userId ?? null, value: JSON.stringify(event) }],
      });
    } catch (err) {
      this.logger.error(`kafka publish failed: ${(err as Error).message}`);
    }
  }
}
