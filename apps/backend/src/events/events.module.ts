import { Global, Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { KAFKA_CLIENT, RABBITMQ_CLIENT } from './event-bus.constants';
import { EventBusService } from './event-bus.service';

// Clients connect lazily (on first emit), so registering them here does not
// require the brokers to be running — important for M1 where only the
// producer exists.
@Global()
@Module({
  imports: [
    ClientsModule.register([
      {
        name: RABBITMQ_CLIENT,
        transport: Transport.RMQ,
        options: {
          urls: [process.env.RABBITMQ_URL ?? 'amqp://localhost:5672'],
          // Publish to a topic exchange; emit(pattern) uses pattern as the
          // routing key. Consumers (M2) bind their own queues to this exchange.
          exchange: 'events',
          exchangeType: 'topic',
          wildcards: true,
          // Producer-only client: it never consumes, so no work queue.
          queue: 'backend-producer-unused',
        },
      },
      {
        name: KAFKA_CLIENT,
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'backend-producer',
            brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
          },
          producerOnlyMode: true,
        },
      },
    ]),
  ],
  providers: [EventBusService],
  exports: [EventBusService],
})
export class EventsModule {}
