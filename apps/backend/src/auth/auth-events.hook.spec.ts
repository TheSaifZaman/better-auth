// The decorators come from an ESM-only package; stub them so ts-jest can load.
jest.mock('@thallesp/nestjs-better-auth', () => ({
  DatabaseHook: () => () => undefined,
  AfterCreate: () => () => undefined,
}));

import { AuthEventsHook } from './auth-events.hook';
import { EVENTS } from '../events/event-bus.constants';

describe('AuthEventsHook', () => {
  it('publishes auth.user.created with the new user id', async () => {
    const events = { publish: jest.fn().mockResolvedValue(undefined) };
    const hook = new AuthEventsHook(events as any);

    await hook.onUserCreated({ id: 'u1', email: 'a@b.c' });

    expect(events.publish).toHaveBeenCalledWith(
      expect.objectContaining({ event: EVENTS.AUTH_USER_CREATED, userId: 'u1' }),
    );
  });

  it('publishes auth.session.created with the session user id', async () => {
    const events = { publish: jest.fn().mockResolvedValue(undefined) };
    const hook = new AuthEventsHook(events as any);

    await hook.onSessionCreated({ id: 's1', userId: 'u1' });

    expect(events.publish).toHaveBeenCalledWith(
      expect.objectContaining({ event: EVENTS.AUTH_SESSION_CREATED, userId: 'u1' }),
    );
  });
});
