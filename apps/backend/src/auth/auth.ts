import { betterAuth } from 'better-auth';
import { drizzleAdapter } from 'better-auth/adapters/drizzle';
import { buildAuthOptions } from './auth-options';

/**
 * Standalone instance for the better-auth CLI (schema generation, etc).
 * The running application builds its own instance in app.module.ts with a
 * real Drizzle connection; both share buildAuthOptions() so they stay in sync.
 */
export const auth = betterAuth({
  database: drizzleAdapter({}, { provider: 'pg' }),
  ...buildAuthOptions(),
});
