import { decideAction } from './retry-policy';

describe('decideAction', () => {
  it('acks a successful message', () => {
    expect(decideAction(true, false)).toBe('ack');
    expect(decideAction(true, true)).toBe('ack');
  });

  it('requeues the first failure', () => {
    expect(decideAction(false, false)).toBe('requeue');
  });

  it('dead-letters a failure that was already redelivered', () => {
    expect(decideAction(false, true)).toBe('deadletter');
  });
});
