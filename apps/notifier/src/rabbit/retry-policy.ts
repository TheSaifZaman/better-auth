export type AckAction = 'ack' | 'requeue' | 'deadletter';

// One bounded retry: a fresh failure is requeued once; a failure that was
// already redelivered is dead-lettered. RabbitMQ's `redelivered` flag is the
// only state we need — no external attempt counter.
export function decideAction(succeeded: boolean, redelivered: boolean): AckAction {
  if (succeeded) return 'ack';
  return redelivered ? 'deadletter' : 'requeue';
}
