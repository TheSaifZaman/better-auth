import { Module } from '@nestjs/common';
import { DatabaseModule } from './database/database.module';
import { NotificationsController } from './notifications/notifications.controller';
import { NotificationService } from './notifications/notification.service';

@Module({
  imports: [DatabaseModule],
  controllers: [NotificationsController],
  providers: [NotificationService],
})
export class AppModule {}
