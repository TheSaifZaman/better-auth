import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

// Replaced in Task 4 with the RMQ microservice bootstrap.
async function bootstrap() {
  await NestFactory.createApplicationContext(AppModule);
}
bootstrap();
