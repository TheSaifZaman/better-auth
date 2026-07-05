import crypto from 'node:crypto';
import type { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import request from 'supertest';
import type { App } from 'supertest/types';
import { AppModule } from '../src/app.module';

/** Boot the full Nest app in-process for e2e tests. */
export async function bootApp(): Promise<INestApplication<App>> {
  const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
  const app = moduleRef.createNestApplication();
  await app.init();
  return app;
}

export const uniqueEmail = (tag = 'user') =>
  `${tag}-${crypto.randomBytes(6).toString('hex')}@example.com`;

/** A distinct client IP per logical actor so per-IP rate-limit buckets don't collide. */
let ipCounter = 0;
export const nextClientIp = () => `203.0.113.${(ipCounter++ % 250) + 1}`;

// --- RFC 4648 base32 decode + RFC 6238 TOTP (matches better-auth defaults) ---
function base32Decode(input: string): Buffer {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  const s = input.replace(/=+$/, '').toUpperCase();
  let bits = '';
  for (const c of s) bits += alphabet.indexOf(c).toString(2).padStart(5, '0');
  const bytes: number[] = [];
  for (let i = 0; i + 8 <= bits.length; i += 8) bytes.push(parseInt(bits.slice(i, i + 8), 2));
  return Buffer.from(bytes);
}

export function totp(secret: string, step = 30, digits = 6, at = Date.now()): string {
  const counter = Math.floor(at / 1000 / step);
  const buf = Buffer.alloc(8);
  buf.writeBigInt64BE(BigInt(counter));
  const hmac = crypto.createHmac('sha1', base32Decode(secret)).update(buf).digest();
  const offset = hmac[hmac.length - 1] & 0xf;
  const code =
    ((hmac[offset] & 0x7f) << 24) |
    ((hmac[offset + 1] & 0xff) << 16) |
    ((hmac[offset + 2] & 0xff) << 8) |
    (hmac[offset + 3] & 0xff);
  return (code % 10 ** digits).toString().padStart(digits, '0');
}

export const secretFromUri = (uri: string) => new URL(uri).searchParams.get('secret')!;

/** Seconds left in the current TOTP window. */
const windowRemaining = (step = 30) => step - Math.floor(Date.now() / 1000) % step;

/** Wait until a brand-new TOTP window starts (so a code can't collide with a prior one). */
export async function waitForFreshWindow(step = 30): Promise<void> {
  await new Promise((r) => setTimeout(r, (windowRemaining(step) + 1) * 1000));
}

/** Ensure at least `min` seconds of headroom in the current window for back-to-back calls. */
export async function ensureHeadroom(min = 5, step = 30): Promise<void> {
  if (windowRemaining(step) < min) await waitForFreshWindow(step);
}

/**
 * Minimal cookie-carrying HTTP client over the Nest server, mimicking a
 * single browser. Each client gets a fixed X-Forwarded-For so rate limiting
 * treats it as one user.
 */
export class Client {
  private cookies = new Map<string, string>();
  constructor(
    private readonly app: INestApplication<App>,
    private readonly ip = nextClientIp(),
    private readonly origin = 'http://localhost:3001',
  ) {}

  private absorb(res: request.Response) {
    const setCookie = (res.headers['set-cookie'] as unknown as string[]) ?? [];
    for (const line of setCookie) {
      const [name, ...rest] = line.split(';')[0].split('=');
      const value = rest.join('=');
      if (value === '' || /Max-Age=0/i.test(line)) this.cookies.delete(name);
      else this.cookies.set(name, value);
    }
    return res;
  }

  private header() {
    return [...this.cookies.entries()].map(([k, v]) => `${k}=${v}`).join('; ');
  }

  post(path: string, body: unknown) {
    return request(this.app.getHttpServer())
      .post(path)
      .set('Origin', this.origin)
      .set('X-Forwarded-For', this.ip)
      .set('Cookie', this.header())
      .send(body as object)
      .then((res) => this.absorb(res));
  }

  get(path: string) {
    return request(this.app.getHttpServer())
      .get(path)
      .set('Origin', this.origin)
      .set('X-Forwarded-For', this.ip)
      .set('Cookie', this.header())
      .then((res) => this.absorb(res));
  }

  hasSession() {
    return [...this.cookies.keys()].some((k) => /session_token/.test(k));
  }
}
