import { Injectable } from '@nestjs/common';
import { AfterCreate, DatabaseHook } from '@thallesp/nestjs-better-auth';
import { EVENTS } from '../events/event-bus.constants';
import { EventBusService } from '../events/event-bus.service';

// Second producer: better-auth database lifecycle events flow through the same
// EventBus as run events. AuthModule auto-discovers @DatabaseHook providers.
@Injectable()
@DatabaseHook()
export class AuthEventsHook {
  constructor(private readonly events: EventBusService) {}

  @AfterCreate('user')
  async onUserCreated(user: Record<string, any>): Promise<void> {
    await this.events.publish({
      event: EVENTS.AUTH_USER_CREATED,
      userId: user?.id,
      at: new Date().toISOString(),
    });
  }

  @AfterCreate('session')
  async onSessionCreated(session: Record<string, any>): Promise<void> {
    await this.events.publish({
      event: EVENTS.AUTH_SESSION_CREATED,
      userId: session?.userId,
      at: new Date().toISOString(),
    });
  }
}
