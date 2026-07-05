import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { DatabaseModule } from './database/database.module';
import { AuthGuard, AuthModule } from '@thallesp/nestjs-better-auth';
import { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { betterAuth } from 'better-auth';
import { drizzleAdapter } from 'better-auth/adapters/drizzle';
import { DATABASE_CONNECTION } from './database/database-coonection';
import { UsersModule } from './users/users.module';
import { APP_GUARD } from '@nestjs/core';
import { buildAuthOptions } from './auth/auth-options';
import { EventsModule } from './events/events.module';
import { RunsModule } from './runs/runs.module';

@Module({
  imports: [
    UsersModule,
    EventsModule,
    RunsModule,
    ConfigModule.forRoot(),
    AuthModule.forRootAsync({
      imports: [DatabaseModule],
      useFactory: (database: NodePgDatabase) => ({
        auth: betterAuth({
          database: drizzleAdapter(database, {
            provider: 'pg',
          }),
          ...buildAuthOptions(),
        }),
      }),
      inject: [DATABASE_CONNECTION],
    }),
  ],
  controllers: [],
  providers: [
    {
      provide: APP_GUARD,
      useClass: AuthGuard,
    },
  ],
})
export class AppModule {}
