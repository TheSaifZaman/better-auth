import { Body, Controller, Param, Post } from '@nestjs/common';
import { Public } from '@thallesp/nestjs-better-auth';
import { RunsService } from './runs.service';

// Public for the learning exercise — no session required to drive runs.
@Public()
@Controller('runs')
export class RunsController {
  constructor(private readonly runs: RunsService) {}

  @Post()
  create(@Body('workflow') workflowName?: string) {
    return this.runs.createRun(workflowName ?? 'default');
  }

  @Post(':id/advance')
  advance(@Param('id') id: string) {
    return this.runs.advance(id);
  }
}
