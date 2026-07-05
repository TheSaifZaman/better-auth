import { Test } from '@nestjs/testing';
import { EventsModule } from './events.module';
import { EventBusService } from './event-bus.service';

describe('EventsModule', () => {
  it('resolves EventBusService with both clients injected', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [EventsModule],
    }).compile();

    const bus = moduleRef.get(EventBusService);
    expect(bus).toBeInstanceOf(EventBusService);
    await moduleRef.close();
  });
});
