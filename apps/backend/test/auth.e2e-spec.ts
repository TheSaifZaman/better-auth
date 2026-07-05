import type { INestApplication } from '@nestjs/common';
import type { App } from 'supertest/types';

// Capture outgoing mail in-memory instead of hitting SMTP/Mailpit.
jest.mock('../src/auth/mailer', () => ({
  __esModule: true,
  sendVerificationEmail: jest.fn(async () => undefined),
  sendResetPasswordEmail: jest.fn(async () => undefined),
  sendMail: jest.fn(async () => undefined),
}));

import * as mailer from '../src/auth/mailer';
import { bootApp, Client, uniqueEmail } from './support';

const sendVerification = mailer.sendVerificationEmail as jest.Mock;
const sendReset = mailer.sendResetPasswordEmail as jest.Mock;

const STRONG = 'Zx9$kQ2mPvL7!wn';
const pathOf = (url: string) => {
  const u = new URL(url);
  return u.pathname + u.search;
};

jest.setTimeout(60_000);

describe('Auth (e2e)', () => {
  let app: INestApplication<App>;

  beforeAll(async () => {
    app = await bootApp();
  });
  afterAll(async () => {
    await app.close();
  });
  beforeEach(() => {
    sendVerification.mockClear();
    sendReset.mockClear();
  });

  it('rejects passwords found in known breaches (HIBP)', async () => {
    const res = await new Client(app).post('/api/auth/sign-up/email', {
      name: 'Breach',
      email: uniqueEmail('hibp'),
      password: 'Password123!', // known-breached
    });
    expect(res.status).toBe(400);
    expect(res.body.code).toBe('PASSWORD_COMPROMISED');
  });

  it('rejects passwords shorter than the minimum length', async () => {
    const res = await new Client(app).post('/api/auth/sign-up/email', {
      name: 'Short',
      email: uniqueEmail('short'),
      password: 'Ab1!x', // 5 chars
    });
    expect(res.status).toBeGreaterThanOrEqual(400);
  });

  it('signs up, sends a verification email, and blocks sign-in until verified', async () => {
    const email = uniqueEmail('verify');
    const client = new Client(app);

    const signup = await client.post('/api/auth/sign-up/email', {
      name: 'Ada',
      email,
      password: STRONG,
      callbackURL: 'http://localhost:3001/',
    });
    expect(signup.status).toBe(200);
    expect(signup.body.user.emailVerified).toBe(false);
    expect(sendVerification).toHaveBeenCalledTimes(1);

    // Sign-in before verification is refused.
    const blocked = await new Client(app).post('/api/auth/sign-in/email', { email, password: STRONG });
    expect(blocked.status).toBe(403);
    expect(blocked.body.code).toBe('EMAIL_NOT_VERIFIED');

    // Follow the emailed link → auto sign-in + verified.
    const [, url] = sendVerification.mock.calls[0];
    const verified = await client.get(pathOf(url));
    expect([200, 302]).toContain(verified.status);
    expect(client.hasSession()).toBe(true);

    const session = await client.get('/api/auth/get-session');
    expect(session.body.user.email).toBe(email);
    expect(session.body.user.emailVerified).toBe(true);
  });

  it('rate-limits repeated failed sign-ins from one IP', async () => {
    const client = new Client(app); // fixed IP for the whole burst
    const email = uniqueEmail('rl');
    const codes: number[] = [];
    for (let i = 0; i < 7; i++) {
      const res = await client.post('/api/auth/sign-in/email', { email, password: `wrong-${i}` });
      codes.push(res.status);
    }
    expect(codes).toContain(429);
  });

  it('supports the full password-reset flow', async () => {
    const email = uniqueEmail('reset');
    const client = new Client(app);

    // Create + verify an account.
    await client.post('/api/auth/sign-up/email', {
      name: 'Reset',
      email,
      password: STRONG,
      callbackURL: 'http://localhost:3001/',
    });
    await client.get(pathOf(sendVerification.mock.calls[0][1]));

    // Request a reset link.
    const forgot = await new Client(app).post('/api/auth/request-password-reset', {
      email,
      redirectTo: 'http://localhost:3001/reset-password',
    });
    expect(forgot.status).toBe(200);
    expect(sendReset).toHaveBeenCalledTimes(1);

    // Extract the token from the emailed URL and set a new password.
    const resetUrl: string = sendReset.mock.calls[0][1];
    const token = resetUrl.match(/reset-password\/([^?/]+)/)?.[1];
    expect(token).toBeTruthy();

    const NEW = 'Qw7#nLp2!xVt9z';
    const reset = await new Client(app).post('/api/auth/reset-password', {
      newPassword: NEW,
      token,
    });
    expect(reset.status).toBe(200);

    // Old password no longer works; new one does.
    const oldTry = await new Client(app).post('/api/auth/sign-in/email', { email, password: STRONG });
    expect(oldTry.status).toBeGreaterThanOrEqual(400);
    const newTry = await new Client(app).post('/api/auth/sign-in/email', { email, password: NEW });
    expect(newTry.status).toBe(200);
  });
});
