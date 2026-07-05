import { advanceStatus, failRun } from './run-state-machine';
import { EVENTS } from '../events/event-bus.constants';

describe('advanceStatus', () => {
  it('queued -> running emits run.advanced', () => {
    expect(advanceStatus('queued')).toEqual({
      next: 'running',
      events: [EVENTS.RUN_ADVANCED],
    });
  });

  it('running -> completed emits run.advanced and run.completed', () => {
    expect(advanceStatus('running')).toEqual({
      next: 'completed',
      events: [EVENTS.RUN_ADVANCED, EVENTS.RUN_COMPLETED],
    });
  });

  it('throws when advancing a completed run', () => {
    expect(() => advanceStatus('completed')).toThrow('run already completed');
  });

  it('throws when advancing a failed run', () => {
    expect(() => advanceStatus('failed')).toThrow('run already failed');
  });
});

describe('failRun', () => {
  it('emits run.failed', () => {
    expect(failRun()).toEqual({ next: 'failed', events: [EVENTS.RUN_FAILED] });
  });
});
