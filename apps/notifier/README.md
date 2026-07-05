# notifier

NestJS **RabbitMQ consumer** for the [better-auth monorepo](../../README.md) — the *smart-broker* half of the dual-broker exercise. A pure microservice (no HTTP): it consumes events the backend publishes and writes `notification` rows.

## What it demonstrates (RabbitMQ = smart broker)

- **Topic exchange + routing keys** — queue `notifications` is bound to the `events` exchange with `run.#` and `auth.#`; the broker routes on the message's routing key.
- **Manual per-message ack** (`noAck: false`) — the handler writes the row, then `channel.ack(msg)`.
- **Bounded retry → DLQ** — on failure the first delivery is `nack`'d with requeue; a **redelivered** failure is dead-lettered (`nack` no-requeue) to `events.dlx` → `notifications.dlq`.
- **Ephemeral** — once a message is acked it is *gone*. There is no replay. (Contrast: `apps/audit-go` replays Kafka from offset 0.)

Nest owns the main exchange/queue/bindings; a small `amqplib` bootstrap (`src/rabbit/topology.ts`) declares the dead-letter side that Nest doesn't know about.

## Environment

```sh
DATABASE_URL=postgres://user:pass@localhost:5432/betterauth
RABBITMQ_URL=amqp://localhost:5672
NOTIFIER_FAIL_ON=run.completed   # optional: force failures for the given routing key (DLQ demo)
```

## Run

```sh
pnpm --filter @repo/db build     # notification table + connection come from @repo/db
pnpm --filter notifier build
node apps/notifier/dist/main.js
pnpm --filter notifier test      # unit tests (retry policy, service, handler ack/nack)
```

## See the DLQ in action

```sh
# start with the failure toggle, publish a matching event, then inspect the DLQ
NOTIFIER_FAIL_ON=run.completed node apps/notifier/dist/main.js &
pnpm --filter notifier exec ts-node scripts/publish-test-event.ts run.completed
curl -s -u guest:guest http://localhost:15673/api/queues/%2F/notifications.dlq | grep -o '"messages":[0-9]*'
```

Logs show `requeue … (first failure)` then `dead-letter … (already retried)`; the message lands in `notifications.dlq` (management UI: http://localhost:15673).

## Key files

- `src/notifications/notifications.controller.ts` — `@EventPattern('run.#'/'auth.#')`, manual ack/nack
- `src/rabbit/retry-policy.ts` — the pure `ack | requeue | deadletter` decision
- `src/rabbit/topology.ts` — dead-letter exchange + queue
- `src/main.ts` — RMQ microservice bootstrap
