import { Global, Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { Kafka } from 'kafkajs';
import { KAFKA_PRODUCER, RABBITMQ_CLIENT } from './event-bus.constants';
import { EventBusService } from './event-bus.service';

@Global()
@Module({
  imports: [
    ClientsModule.register([
      {
        name: RABBITMQ_CLIENT,
        transport: Transport.RMQ,
        options: {
          urls: [process.env.RABBITMQ_URL ?? 'amqp://localhost:5672'],
          exchange: 'events',
          exchangeType: 'topic',
          wildcards: true,
          queue: 'backend-producer-unused',
        },
      },
    ]),
  ],
  providers: [
    {
      // Raw kafkajs producer: the Kafka consumer is a Go service that expects a
      // plain-JSON value, so we bypass Nest's Kafka transport envelope.
      provide: KAFKA_PRODUCER,
      useFactory: () => {
        const kafka = new Kafka({
          clientId: 'backend-producer',
          brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
        });
        return kafka.producer();
      },
    },
    EventBusService,
  ],
  exports: [EventBusService],
})
export class EventsModule {}
