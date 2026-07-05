import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { assertDeadLetterTopology } from './rabbit/topology';
import {
  DLX_EXCHANGE,
  EVENTS_EXCHANGE,
  NOTIFICATIONS_QUEUE,
} from './rabbit/rabbit.constants';

async function bootstrap() {
  const url = process.env.RABBITMQ_URL ?? 'amqp://localhost:5672';

  // Own the dead-letter side before Nest asserts the main queue/exchange.
  await assertDeadLetterTopology(url);

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.RMQ,
    options: {
      urls: [url],
      exchange: EVENTS_EXCHANGE,
      exchangeType: 'topic',
      wildcards: true,
      queue: NOTIFICATIONS_QUEUE,
      noAck: false,
      queueOptions: {
        durable: true,
        arguments: { 'x-dead-letter-exchange': DLX_EXCHANGE },
      },
    },
  });

  await app.listen();
  new Logger('Bootstrap').log('notifier listening');
}

bootstrap();
