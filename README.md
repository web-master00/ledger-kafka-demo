# Reliable Ledger Events: Node.js + TypeScript + PostgreSQL + Kafka

A client-facing demo of the transactional outbox pattern. The API atomically saves a transaction and its outbox event in PostgreSQL. A publisher sends outbox events to Kafka. A consumer writes a ledger entry and records the processed event in one database transaction, then commits the Kafka offset.

> **Demo, not production financial software.** It has a single Kafka broker, simplified outbox polling, no authentication/TLS, and no production-grade retry/DLQ policy. Do not use it to process real money or real customer data.

## Requirements

- Docker Desktop / Docker Engine with Compose
- Node.js 20+ (22 recommended)
- npm

## Start

```bash
docker compose up -d
# Wait until both services are healthy
docker compose ps
npm install
```

Create `.env` from `.env.example`. In PowerShell:

```powershell
Copy-Item .env.example .env
```

Open three terminals in this folder:

```bash
npm run dev:api
```

```bash
npm run dev:publisher
```

```bash
npm run dev:consumer
```

The API and publisher create the `ledger.transactions` topic if missing.

## Submit a transaction

Linux/macOS/Git Bash:

```bash
curl -X POST http://localhost:3000/transactions   -H 'Content-Type: application/json'   -d '{"idempotencyKey":"demo-payment-0001","accountId":"acct-demo-42","amountMinor":12500,"currency":"USD"}'
```

PowerShell:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:3000/transactions -ContentType 'application/json' -Body '{"idempotencyKey":"demo-payment-0001","accountId":"acct-demo-42","amountMinor":12500,"currency":"USD"}'
```

`amountMinor` is expressed in minor currency units: `12500` means USD 125.00. The API returns `202 Accepted` after the database transaction commits; it does not claim that the ledger consumer has finished.

Send the same request again with the same idempotency key to see the API return the original transaction rather than create another one. Use a new key for a new transaction.

## Inspect the results

```bash
docker exec -it ledger-kafka-demo-postgres-1 psql -U demo -d ledger_demo -c 'SELECT id, idempotency_key, status, amount_minor, currency FROM transactions ORDER BY created_at DESC;'
docker exec -it ledger-kafka-demo-postgres-1 psql -U demo -d ledger_demo -c 'SELECT transaction_id, account_id, amount_minor, currency FROM ledger_entries ORDER BY created_at DESC;'
docker exec -it ledger-kafka-demo-postgres-1 psql -U demo -d ledger_demo -c 'SELECT id, published_at, attempts, last_error FROM outbox_events ORDER BY created_at DESC;'
```

Container names can vary by Compose project name. If the `psql` command's container name differs, find it with `docker compose ps` and replace the name.

Inspect Kafka topic:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ledger.transactions
```

Read the topic from the beginning in another terminal:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ledger.transactions --from-beginning
```

## Live demo script (7–10 minutes)

1. Explain the risk with in-memory background tasks.
2. Show the database transaction that writes both `transactions` and `outbox_events` atomically.
3. Start API, publisher, and consumer terminals.
4. Submit a transaction and follow its event through the outbox publisher into Kafka and then into `ledger_entries`.
5. Repeat the request with the same idempotency key and show that no second transaction is created.
6. Stop the consumer, submit a new transaction, and confirm the outbox publisher still publishes the event. Restart the consumer and show it processes pending records.
7. Explain why idempotency is necessary even when Kafka retains messages.

## Architecture and guarantees

- **Atomic acceptance:** transaction row and outbox row are committed in one PostgreSQL transaction. The event cannot be forgotten merely because the API process stops after commit.
- **Outbox publisher:** retries publishing rows whose `published_at` is null. A crash after Kafka accepts a message but before PostgreSQL marks it published can cause a duplicate publish; consumers must be idempotent.
- **Kafka:** buffers and retains published events according to broker configuration. This single-broker demo does not provide broker-level high availability.
- **Consumer idempotency:** the consumer writes `ledger_entries` and `processed_events` in one DB transaction. A unique constraint on `transaction_id` prevents duplicate ledger entries. The Kafka offset is committed only after the DB transaction commits.
- **Delivery semantics:** this is effectively at-least-once delivery with idempotent database effects, not a claim of global exactly-once processing.
- **Ordering:** the transaction ID is the Kafka message key, so events for a transaction route consistently to one partition. Ordering is per partition, not global.

## Known limitations / production follow-up

This is intentionally compact. Before production use, improve the outbox publisher with safe row claiming/leases and concurrency controls; handle idempotency-key race conditions explicitly; add bounded retry/backoff and dead-letter/replay workflows; validate event schemas and versions; add auth/TLS, secrets management, metrics, tracing, alerts, database migrations, graceful shutdown, integration tests, and multi-broker Kafka replication. Financial ledger correctness also requires domain-specific accounting invariants, reconciliation, and audit controls.
