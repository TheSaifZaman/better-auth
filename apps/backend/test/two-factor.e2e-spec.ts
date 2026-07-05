import type { INestApplication } from '@nestjs/common';
import type { App } from 'supertest/types';

jest.mock('../src/auth/mailer', () => ({
  __esModule: true,
  sendVerificationEmail: jest.fn(async () => undefined),
  sendResetPasswordEmail: jest.fn(async () => undefined),
  sendMail: jest.fn(async () => undefined),
}));

import * as mailer from '../src/auth/mailer';
import {
  bootApp,
  Client,
  ensureHeadroom,
  secretFromUri,
  totp,
  uniqueEmail,
  waitForFreshWindow,
} from './support';

const sendVerification = mailer.sendVerificationEmail as jest.Mock;
const STRONG = 'Zx9$kQ2mPvL7!wn';
const pathOf = (url: string) => {
  const u = new URL(url);
  return u.pathname + u.search;
};

jest.setTimeout(180_000);

/** Create a verified, signed-in client. */
async function signUpAndVerify(app: INestApplication<App>, tag: string) {
  const email = uniqueEmail(tag);
  const client = new Client(app);
  sendVerification.mockClear();
  await client.post('/api/auth/sign-up/email', {
    name: tag,
    email,
    password: STRONG,
    callbackURL: 'http://localhost:3001/',
  });
  await client.get(pathOf(sendVerification.mock.calls[0][1]));
  return { email, client };
}

describe('Two-factor auth (e2e)', () => {
  let app: INestApplication<App>;
  let owner: Client;
  let email: string;
  let secret: string;
  let backupCodes: string[];

  beforeAll(async () => {
    app = await bootApp();
    // Set up one account with 2FA fully enabled, reused across tests.
    const created = await signUpAndVerify(app, 'tfa-owner');
    owner = created.client;
    email = created.email;

    const enable = await owner.post('/api/auth/two-factor/enable', { password: STRONG });
    secret = secretFromUri(enable.body.totpURI);
    backupCodes = enable.body.backupCodes;

    await waitForFreshWindow();
    await owner.post('/api/auth/two-factor/verify-totp', { code: totp(secret) });
  });

  afterAll(async () => {
    await app.close();
  });

  it('issues a totpURI and exactly 10 backup codes on enable', () => {
    expect(secret).toMatch(/^[A-Z2-7]+$/);
    expect(Array.isArray(backupCodes)).toBe(true);
    expect(backupCodes).toHaveLength(10);
  });

  it('marks the account twoFactorEnabled after activation', async () => {
    const session = await owner.get('/api/auth/get-session');
    expect(session.body.user.twoFactorEnabled).toBe(true);
  });

  it('never exposes the 2FA secret or backup codes in the session', async () => {
    const session = await owner.get('/api/auth/get-session');
    const blob = JSON.stringify(session.body);
    expect(blob).not.toContain(secret);
    expect(session.body.user.secret).toBeUndefined();
    expect(session.body.user.backupCodes).toBeUndefined();
  });

  it('rejects enable with the wrong password', async () => {
    const { client } = await signUpAndVerify(app, 'tfa-badpw');
    const res = await client.post('/api/auth/two-factor/enable', { password: 'not-my-password' });
    expect(res.status).toBeGreaterThanOrEqual(400);
    expect(res.body.totpURI).toBeUndefined();
  });

  it('requires a second factor at sign-in (password alone is not a full session)', async () => {
    const login = new Client(app);
    const res = await login.post('/api/auth/sign-in/email', { email, password: STRONG });
    expect(res.body.twoFactorRedirect).toBe(true);

    // The partial session must not satisfy the global AuthGuard.
    const protectedRes = await login.get('/users/session');
    expect(protectedRes.status).toBe(401);
  });

  it('rejects an invalid TOTP code at login', async () => {
    const login = new Client(app);
    await login.post('/api/auth/sign-in/email', { email, password: STRONG });
    const res = await login.post('/api/auth/two-factor/verify-totp', { code: '000000' });
    expect(res.status).toBeGreaterThanOrEqual(400);
    expect(login.hasSession()).toBe(false);
  });

  it('grants a full session for a valid TOTP', async () => {
    await waitForFreshWindow(); // separate window from setup/activation
    await ensureHeadroom(10);
    const code = totp(secret);

    const login = new Client(app);
    await login.post('/api/auth/sign-in/email', { email, password: STRONG });
    const good = await login.post('/api/auth/two-factor/verify-totp', { code });
    expect(good.status).toBe(200);
    const session = await login.get('/users/session');
    expect(session.status).toBe(200);
  });

  // Our totpReplayGuard plugin enforces per-user single-use: a valid TOTP that
  // succeeded once cannot be replayed on a separate sign-in while still in its
  // window. (Without the guard, better-auth 1.6.23 would accept the reuse.)
  it('rejects replay of a valid TOTP across a separate sign-in (single-use guard)', async () => {
    await waitForFreshWindow();
    await ensureHeadroom(10);
    const code = totp(secret);

    const first = new Client(app);
    await first.post('/api/auth/sign-in/email', { email, password: STRONG });
    expect((await first.post('/api/auth/two-factor/verify-totp', { code })).status).toBe(200);

    const second = new Client(app);
    await second.post('/api/auth/sign-in/email', { email, password: STRONG });
    const reuse = await second.post('/api/auth/two-factor/verify-totp', { code });
    expect(reuse.status).toBeGreaterThanOrEqual(400);
    expect(second.hasSession()).toBe(false);
  });

  it('accepts a backup code once and rejects its reuse', async () => {
    const code = backupCodes[0];

    const first = new Client(app);
    await first.post('/api/auth/sign-in/email', { email, password: STRONG });
    const firstRes = await first.post('/api/auth/two-factor/verify-backup-code', { code });
    expect(firstRes.status).toBe(200);
    expect(first.hasSession()).toBe(true);

    const second = new Client(app);
    await second.post('/api/auth/sign-in/email', { email, password: STRONG });
    const secondRes = await second.post('/api/auth/two-factor/verify-backup-code', { code });
    expect(secondRes.status).toBeGreaterThanOrEqual(400);
    expect(second.hasSession()).toBe(false);
  });

  it('disables 2FA only with the correct password and stops requiring it', async () => {
    const bad = await owner.post('/api/auth/two-factor/disable', { password: 'wrong' });
    expect(bad.status).toBeGreaterThanOrEqual(400);

    const good = await owner.post('/api/auth/two-factor/disable', { password: STRONG });
    expect(good.status).toBe(200);

    const session = await owner.get('/api/auth/get-session');
    expect(session.body.user.twoFactorEnabled).toBe(false);

    // Sign-in no longer needs a second factor.
    const login = new Client(app);
    const res = await login.post('/api/auth/sign-in/email', { email, password: STRONG });
    expect(res.body.twoFactorRedirect).toBeUndefined();
    expect(login.hasSession()).toBe(true);
  });
});
