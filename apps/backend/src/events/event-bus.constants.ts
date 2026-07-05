// DI tokens for the two producer clients.
export const RABBITMQ_CLIENT = 'RABBITMQ_CLIENT';
export const KAFKA_CLIENT = 'KAFKA_CLIENT';

// Logical event names. On RabbitMQ these are the topic-exchange routing keys.
export const EVENTS = {
  RUN_ADVANCED: 'run.advanced',
  RUN_COMPLETED: 'run.completed',
  RUN_FAILED: 'run.failed',
} as const;

export type EventName = (typeof EVENTS)[keyof typeof EVENTS];

// The single payload shape published to BOTH brokers.
export interface RunEventPayload {
  event: EventName;
  runId: string;
  workflowId: string;
  status: string;
  step: number;
  at: string; // ISO-8601 timestamp
}
