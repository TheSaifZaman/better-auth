import type { INestApplication } from '@nestjs/common';
import request from 'supertest';
import type { App } from 'supertest/types';
import { bootApp } from './support';

describe('App bootstrap (e2e)', () => {
  let app: INestApplication<App>;

  beforeAll(async () => {
    app = await bootApp();
  });
  afterAll(async () => {
    await app.close();
  });

  it('boots and mounts better-auth under /api/auth', async () => {
    // No session cookie → get-session responds with a null session, not an error.
    const res = await request(app.getHttpServer()).get('/api/auth/get-session');
    expect(res.status).toBe(200);
  });

  it('protects app routes with the global AuthGuard', async () => {
    const res = await request(app.getHttpServer()).get('/users/session');
    expect(res.status).toBe(401);
  });
});
