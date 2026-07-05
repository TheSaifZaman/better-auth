import { Module } from '@nestjs/common';
import { DatabaseModule } from '../database/database.module';
import { RunsController } from './runs.controller';
import { RunsService } from './runs.service';

@Module({
  imports: [DatabaseModule],
  controllers: [RunsController],
  providers: [RunsService],
})
export class RunsModule {}
