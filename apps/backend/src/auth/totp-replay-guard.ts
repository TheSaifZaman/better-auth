import type { BetterAuthPlugin } from 'better-auth';
import { APIError, createAuthMiddleware } from 'better-auth/api';

/**
 * Per-user single-use enforcement for TOTP codes.
 *
 * better-auth (1.6.23) verifies a TOTP against the time window but does not
 * remember which codes were already consumed, so a still-valid code can be
 * replayed across separate sign-in attempts until it expires. This plugin
 * closes that gap: on `/two-factor/verify-totp` it resolves the pending user
 * (the same signed-cookie path better-auth uses internally) and rejects a
 * `(user, code)` pair that was already seen, marking it atomically so a
 * concurrent replay loses the race.
 *
 * Backup codes are already single-use (consumed on verification), so only the
 * TOTP path needs this.
 */
const TWO_FACTOR_COOKIE_NAME = 'two_factor';
// A used marker only needs to outlive the code: one period + generous skew.
const USED_TTL_MS = 90_000;

export const totpReplayGuard = (): BetterAuthPlugin => ({
  id: 'totp-replay-guard',
  hooks: {
    before: [
      {
        matcher: (ctx) => ctx.path === '/two-factor/verify-totp',
        handler: createAuthMiddleware(async (ctx) => {
          const code = (ctx.body as { code?: string } | undefined)?.code;
          if (!code) return;

          // Resolve the pending user from the signed 2FA cookie. If there is
          // no such cookie (e.g. activating 2FA while fully logged in), there
          // is no cross-session replay vector, so skip.
          const cookie = ctx.context.createAuthCookie(TWO_FACTOR_COOKIE_NAME);
          const signed = await ctx.getSignedCookie(cookie.name, ctx.context.secret);
          if (!signed) return;

          const pending = await ctx.context.internalAdapter.findVerificationValue(signed);
          const userId = pending?.value;
          if (!userId) return;

          const marker = `totp-used:${userId}:${code}`;
          const now = Date.now();

          const seen = await ctx.context.internalAdapter.findVerificationValue(marker);
          if (seen && (!seen.expiresAt || new Date(seen.expiresAt).getTime() > now)) {
            throw new APIError('UNAUTHORIZED', {
              message: 'This code was already used. Wait for a new code from your authenticator.',
              code: 'TOTP_CODE_ALREADY_USED',
            });
          }

          // Reserve it before verification runs so a concurrent replay is blocked.
          await ctx.context.internalAdapter.createVerificationValue({
            identifier: marker,
            value: String(userId),
            expiresAt: new Date(now + USED_TTL_MS),
          });
        }),
      },
    ],
  },
});
