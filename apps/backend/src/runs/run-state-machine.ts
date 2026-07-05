import { EVENTS, EventName } from '../events/event-bus.constants';

export type RunStatus = 'queued' | 'running' | 'completed' | 'failed';

export interface Transition {
  next: RunStatus;
  events: EventName[];
}

// Advancing steps a run forward one stage and reports which events to publish.
export function advanceStatus(current: RunStatus): Transition {
  switch (current) {
    case 'queued':
      return { next: 'running', events: [EVENTS.RUN_ADVANCED] };
    case 'running':
      return {
        next: 'completed',
        events: [EVENTS.RUN_ADVANCED, EVENTS.RUN_COMPLETED],
      };
    case 'completed':
      throw new Error('run already completed');
    case 'failed':
      throw new Error('run already failed');
  }
}

export function failRun(): Transition {
  return { next: 'failed', events: [EVENTS.RUN_FAILED] };
}
