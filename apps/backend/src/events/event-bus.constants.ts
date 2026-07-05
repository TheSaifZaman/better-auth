// DI tokens for the two producer transports.
export const RABBITMQ_CLIENT = 'RABBITMQ_CLIENT';
export const KAFKA_PRODUCER = 'KAFKA_PRODUCER';

// Logical event names. On RabbitMQ these are the topic-exchange routing keys.
export const EVENTS = {
  RUN_ADVANCED: 'run.advanced',
  RUN_COMPLETED: 'run.completed',
  RUN_FAILED: 'run.failed',
  AUTH_USER_CREATED: 'auth.user.created',
  AUTH_SESSION_CREATED: 'auth.session.created',
} as const;

export type EventName = (typeof EVENTS)[keyof typeof EVENTS];

// The single payload published to BOTH brokers. Run events fill the run fields;
// auth events fill userId. `event` and `at` are always present.
export interface DomainEvent {
  event: EventName;
  at: string; // ISO-8601
  runId?: string;
  workflowId?: string;
  status?: string;
  step?: number;
  userId?: string;
}
