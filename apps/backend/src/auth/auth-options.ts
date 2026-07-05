import type { BetterAuthOptions } from 'better-auth';
import { twoFactor, haveIBeenPwned } from 'better-auth/plugins';
import { sendResetPasswordEmail, sendVerificationEmail } from './mailer';
import { totpReplayGuard } from './totp-replay-guard';

/**
 * Security configuration shared by the running NestJS app and the
 * better-auth CLI (schema generation). The `database` adapter is supplied
 * separately by each caller.
 */
export function buildAuthOptions(): Omit<BetterAuthOptions, 'database'> {
  return {
    baseURL: process.env.BETTER_AUTH_URL ?? 'http://localhost:3000',
    secret: process.env.BETTER_AUTH_SECRET,
    trustedOrigins: (process.env.TRUSTED_ORIGINS ?? 'http://localhost:3001').split(','),

    emailAndPassword: {
      enabled: true,
      // Users must confirm their address before their first sign-in.
      requireEmailVerification: true,
      autoSignIn: false,
      minPasswordLength: 8,
      maxPasswordLength: 128,
      resetPasswordTokenExpiresIn: 60 * 60, // 1 hour
      sendResetPassword: async ({ user, url }) => {
        await sendResetPasswordEmail(user.email, url);
      },
    },

    emailVerification: {
      sendOnSignUp: true,
      autoSignInAfterVerification: true,
      expiresIn: 60 * 60, // 1 hour
      sendVerificationEmail: async ({ user, url }) => {
        await sendVerificationEmail(user.email, url);
      },
    },

    // Required (even empty) so nestjs-better-auth can wire discovered
    // @DatabaseHook providers (AuthEventsHook) into Better Auth.
    databaseHooks: {},

    session: {
      expiresIn: 60 * 60 * 24 * 7, // 7 days
      updateAge: 60 * 60 * 24, // refresh once per day
      // Short-lived signed cookie cache avoids a DB hit on every request.
      cookieCache: { enabled: true, maxAge: 5 * 60 },
    },

    account: {
      accountLinking: { enabled: true },
    },

    advanced: {
      // Generate real UUIDs for all primary keys (columns are native `uuid`).
      database: {
        generateId: 'uuid',
      },
      cookiePrefix: 'better-auth',
      useSecureCookies: process.env.NODE_ENV === 'production',
      defaultCookieAttributes: {
        httpOnly: true,
        sameSite: 'lax',
      },
      // Requests arrive via the Next.js rewrite proxy, which forwards the
      // real client IP in x-forwarded-for. Without this, rate limiting can't
      // resolve a client and falls back to one shared bucket for everyone.
      ipAddress: {
        ipAddressHeaders: ['x-forwarded-for'],
      },
    },

    // Brute-force protection. Global bucket plus stricter per-endpoint rules.
    rateLimit: {
      enabled: true,
      window: 60,
      max: 100,
      customRules: {
        '/sign-in/email': { window: 60, max: 5 },
        '/sign-up/email': { window: 60, max: 5 },
        '/two-factor/verify-totp': { window: 60, max: 5 },
        '/two-factor/verify-backup-code': { window: 60, max: 5 },
        '/request-password-reset': { window: 60, max: 3 },
      },
    },

    plugins: [
      twoFactor({
        issuer: process.env.TWO_FACTOR_ISSUER ?? 'Better Auth App',
      }),
      haveIBeenPwned({
        customPasswordCompromisedMessage:
          'This password has appeared in a known data breach. Please choose a different one.',
      }),
      totpReplayGuard(),
    ],
  };
}
