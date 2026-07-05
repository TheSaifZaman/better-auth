#!/usr/bin/env bash
# Drives both pipelines end-to-end against the isolated demo stack.
set -euo pipefail
cd "$(dirname "$0")/.."

DEMO_DB="postgres://postgres:postgres@localhost:5433/betterauth"
export DATABASE_URL="$DEMO_DB"
export RABBITMQ_URL="amqp://localhost:5672"
export KAFKA_BROKERS="localhost:9092"
export BETTER_AUTH_SECRET="${BETTER_AUTH_SECRET:-demo-secret-please-change}"
export BETTER_AUTH_URL="http://localhost:3000"
export TRUSTED_ORIGINS="http://localhost:3000"
export SMTP_HOST="localhost"
export SMTP_PORT="1025"
export MAIL_FROM="demo@example.com"
export PORT=3000

pids=()
cleanup() {
  echo "--- cleanup ---"
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  docker rm -f demo-audit >/dev/null 2>&1 || true
  docker compose down >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== infra up ==="
docker compose up -d
until [ "$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q kafka)")" = healthy ]; do sleep 3; done
until [ "$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q postgres-demo)")" = healthy ]; do sleep 2; done
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic run-events --partitions 3 --replication-factor 1 2>/dev/null || true
sleep 4
DATABASE_URL="$DEMO_DB" pnpm --filter @repo/db exec drizzle-kit migrate
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "truncate audit_log, workflow_run_counts, notification;" || true

echo "=== build ==="
pnpm --filter @repo/db build
pnpm --filter backend build
pnpm --filter notifier build

echo "=== start backend + notifier (host) and audit-go (container) ==="
node apps/backend/dist/main.js & pids+=("$!")
node apps/notifier/dist/main.js & pids+=("$!")
docker rm -f demo-audit >/dev/null 2>&1 || true
docker run -d --name demo-audit --network better-auth_default \
  -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache-25:/go/pkg/mod golang:1.25 \
  go run ./cmd/audit --lib kafka-go --from-beginning --group demo-audit --brokers kafka:29092 \
  --snapshot-every 2s --database-url "postgres://postgres:postgres@postgres-demo:5432/betterauth?sslmode=disable" >/dev/null

echo "=== wait for backend to accept requests ==="
for i in $(seq 1 30); do
  if curl -s -o /dev/null -X POST http://localhost:3000/runs -H 'content-type: application/json' -d '{"workflow":"_warmup"}'; then
    break
  fi
  sleep 1
done
sleep 3

echo "=== drive the run pipeline ==="
RUN_ID=$(curl -s -X POST http://localhost:3000/runs -H 'content-type: application/json' -d '{"workflow":"demo"}' | sed -E 's/.*"id":"([^"]+)".*/\1/')
echo "created run $RUN_ID"
curl -s -X POST "http://localhost:3000/runs/$RUN_ID/advance" >/dev/null   # queued -> running (run.advanced)
curl -s -X POST "http://localhost:3000/runs/$RUN_ID/advance" >/dev/null   # running -> completed (run.advanced + run.completed)

echo "=== trigger an auth event (sign-up) ==="
# Random, non-breached password: the haveIBeenPwned plugin rejects common ones.
DEMO_PW="Zx9-$(openssl rand -hex 12)-Qw2"
curl -s -X POST http://localhost:3000/api/auth/sign-up/email -H 'content-type: application/json' \
  -d "{\"email\":\"demo+$RANDOM@example.com\",\"password\":\"$DEMO_PW\",\"name\":\"Demo\"}" >/dev/null || true

sleep 6

echo "=== RabbitMQ pipeline: notification rows ==="
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "select type, run_id from notification order by created_at;"
echo "=== Kafka pipeline: audit_log ==="
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "select event_type, workflow_id, kafka_partition, kafka_offset from audit_log order by kafka_partition, kafka_offset;"
echo "=== Kafka pipeline: workflow_run_counts ==="
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "select workflow_id, completed_count from workflow_run_counts;"
echo "=== replay from offset 0 (reconstruct counts) ==="
docker run --rm --network better-auth_default -v "$PWD/apps/audit-go":/src -w /src \
  -v audit-go-cache-25:/go/pkg/mod golang:1.25 \
  go run ./cmd/audit --replay --lib sarama --brokers kafka:29092 --replay-idle 5s 2>&1 | grep -E "rebuilt|  " | tail -6

echo "=== demo complete ==="
