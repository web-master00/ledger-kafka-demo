# Client walkthrough: reliable ledger event processing

## Opening

"I prepared a small event-driven ledger demo that shows why Kafka alone isn't the complete reliability solution. The important design is the combination of an atomic database outbox, durable messaging, and idempotent consumers."

## Architecture

1. The API validates a request and checks its idempotency key.
2. One PostgreSQL transaction writes both the transaction and an outbox event.
3. A separate publisher sends outbox events to `ledger.transactions` in Kafka.
4. A consumer writes a ledger entry and records the event ID in the same database transaction.
5. Only after the database commit does the consumer commit its Kafka offset.

## Live walkthrough

- Submit a transaction and trace its IDs through API, outbox publisher, Kafka, and consumer logs.
- Repeat the request with the same idempotency key; show that it reuses the existing transaction.
- Stop the consumer, submit a new transaction, and restart the consumer to show pending work is processed later.
- Inspect `ledger_entries` and `outbox_events` in PostgreSQL.

## Questions to be ready for

**Why an outbox?** A direct database write followed by a Kafka publish creates a dual-write gap. The outbox event is committed atomically with the business record, and a separate publisher retries delivery.

**Can duplicate messages occur?** Yes. The publisher may crash after Kafka accepts a message but before marking the outbox row published. Idempotent consumers are still required.

**Is this exactly-once?** No end-to-end exactly-once claim. The design uses at-least-once delivery with idempotent database effects.

**What about poison messages?** A production implementation needs bounded retries, exponential backoff, a dead-letter/replay policy, and alerts. The demo keeps this simple so the core pattern is visible.

**What makes it production-ready?** Multi-broker Kafka, authentication/TLS, schema evolution, safe outbox leasing, race-safe idempotency, migrations, integration tests, metrics/tracing, operational runbooks, and domain-level reconciliation.

## Be precise

Describe this as a demonstration repository prepared to illustrate the architecture. Do not present it as an existing production system or claim real-money processing.
