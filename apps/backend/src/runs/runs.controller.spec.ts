import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import request from 'supertest';

// nestjs-better-auth ships ESM only, which ts-jest (CommonJS) cannot parse.
// The controller imports @Public() from it purely as route metadata, so stub
// the decorator here to keep Jest from loading the ESM module.
jest.mock('@thallesp/nestjs-better-auth', () => ({
  Public: () => () => undefined,
}));

import { RunsController } from './runs.controller';
import { RunsService } from './runs.service';

describe('RunsController', () => {
  let app: INestApplication;
  const service = {
    createRun: jest
      .fn()
      .mockResolvedValue({ id: 'r1', workflowId: 'w1', status: 'queued', step: 0 }),
    advance: jest
      .fn()
      .mockResolvedValue({ id: 'r1', workflowId: 'w1', status: 'running', step: 1 }),
  };

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      controllers: [RunsController],
      providers: [{ provide: RunsService, useValue: service }],
    }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
  });

  afterAll(async () => await app.close());

  it('POST /runs creates a run', async () => {
    await request(app.getHttpServer())
      .post('/runs')
      .send({ workflow: 'demo' })
      .expect(201);
    expect(service.createRun).toHaveBeenCalledWith('demo');
  });

  it('POST /runs/:id/advance advances a run', async () => {
    await request(app.getHttpServer()).post('/runs/r1/advance').expect(201);
    expect(service.advance).toHaveBeenCalledWith('r1');
  });
});
