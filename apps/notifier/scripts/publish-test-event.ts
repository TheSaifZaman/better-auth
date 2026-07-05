import * as amqp from 'amqplib';
import { EVENTS_EXCHANGE } from '../src/rabbit/rabbit.constants';

// Publishes a Nest-enveloped message ({ pattern, data }) to the events
// exchange with the given routing key. Usage: ts-node publish-test-event.ts run.advanced
async function main() {
  const url = process.env.RABBITMQ_URL ?? 'amqp://localhost:5672';
  const routingKey = process.argv[2] ?? 'run.advanced';
  const data = {
    event: routingKey,
    runId: process.env.TEST_RUN_ID ?? null,
    workflowId: process.env.TEST_WORKFLOW_ID ?? null,
    status: 'running',
    step: 1,
    at: new Date().toISOString(),
  };
  const connection = await amqp.connect(url);
  const channel = await connection.createChannel();
  await channel.assertExchange(EVENTS_EXCHANGE, 'topic', { durable: true });
  channel.publish(
    EVENTS_EXCHANGE,
    routingKey,
    Buffer.from(JSON.stringify({ pattern: routingKey, data })),
  );
  await channel.close();
  await connection.close();
  console.log(`published ${routingKey}`);
}
main();
