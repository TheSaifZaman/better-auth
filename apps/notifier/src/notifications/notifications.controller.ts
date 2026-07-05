import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, Payload, RmqContext } from '@nestjs/microservices';
import { decideAction } from '../rabbit/retry-policy';
import { NotificationService } from './notification.service';
import type { IncomingEvent } from './notification.service';

@Controller()
export class NotificationsController {
  private readonly logger = new Logger(NotificationsController.name);
  // Set NOTIFIER_FAIL_ON=<routingKey> to force failures for the DLQ demo.
  private readonly failOn = process.env.NOTIFIER_FAIL_ON;

  constructor(private readonly notifications: NotificationService) {}

  @EventPattern('run.#')
  handleRunEvent(@Payload() data: IncomingEvent, @Ctx() context: RmqContext) {
    return this.consume(data, context);
  }

  @EventPattern('auth.#')
  handleAuthEvent(@Payload() data: IncomingEvent, @Ctx() context: RmqContext) {
    return this.consume(data, context);
  }

  private async consume(data: IncomingEvent, context: RmqContext): Promise<void> {
    const channel = context.getChannelRef();
    const message = context.getMessage();
    const routingKey: string = message.fields.routingKey;
    const redelivered = Boolean(message.fields.redelivered);

    let succeeded = true;
    try {
      if (this.failOn && routingKey === this.failOn) {
        throw new Error(`simulated failure for ${routingKey}`);
      }
      await this.notifications.record(data);
    } catch (err) {
      succeeded = false;
      this.logger.warn(`processing failed for ${routingKey}: ${(err as Error).message}`);
    }

    switch (decideAction(succeeded, redelivered)) {
      case 'ack':
        channel.ack(message);
        break;
      case 'requeue':
        this.logger.warn(`requeue ${routingKey} (first failure)`);
        channel.nack(message, false, true);
        break;
      case 'deadletter':
        this.logger.error(`dead-letter ${routingKey} (already retried)`);
        channel.nack(message, false, false);
        break;
    }
  }
}
