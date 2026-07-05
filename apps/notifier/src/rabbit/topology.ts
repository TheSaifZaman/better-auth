import * as amqp from 'amqplib';
import { DLX_EXCHANGE, NOTIFICATIONS_DLQ } from './rabbit.constants';

// Nest asserts the main `events` exchange and `notifications` queue (with its
// x-dead-letter-exchange argument) plus the run.#/auth.# bindings. It knows
// nothing about the dead-letter side, so we assert that ourselves before the
// microservice starts.
export async function assertDeadLetterTopology(url: string): Promise<void> {
  const connection = await amqp.connect(url);
  const channel = await connection.createChannel();
  await channel.assertExchange(DLX_EXCHANGE, 'fanout', { durable: true });
  await channel.assertQueue(NOTIFICATIONS_DLQ, { durable: true });
  await channel.bindQueue(NOTIFICATIONS_DLQ, DLX_EXCHANGE, '');
  await channel.close();
  await connection.close();
}
