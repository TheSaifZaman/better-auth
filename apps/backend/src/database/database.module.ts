import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { createDatabase } from '@repo/db';
import { DATABASE_CONNECTION } from './database-coonection';

@Module({
  imports: [ConfigModule],
  providers: [
    {
      provide: DATABASE_CONNECTION,
      useFactory: (configService: ConfigService) =>
        createDatabase(configService.getOrThrow('DATABASE_URL')),
      inject: [ConfigService],
    },
  ],
  exports: [DATABASE_CONNECTION],
})
export class DatabaseModule {}
