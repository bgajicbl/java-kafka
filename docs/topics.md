# Topic Design

> Ledger and CDC topics only. The complete list (payments, revenue, DLQs) is in
> [`event-catalog.md`](event-catalog.md). All money fields are Avro `decimal(19,4)`.

## Core Topics

- `tx-events`
  - Key: `tenantId|accountId`
  - Value: Avro `TransactionEvent`
  - Partitions: 6
  - Use: Source-of-truth input stream for payment and ledger mutations.
  - Re-keyed and repartitioned by `tenantId|accountId` inside the app, so the ledger does not
    depend on producers keying correctly.

- `ledger-dlq`
  - Key: `tenantId|accountId`
  - Value: Avro `TransactionEvent` (the rejected event), reason in the `dlq.reason` header
  - Partitions: 3
  - Use: transactions rejected by the ledger guard (currency differs from the account's currency).

- `ledger-balances`
  - Key: `tenantId|accountId`
  - Value: Avro `LedgerBalance`
  - Partitions: 6
  - Cleanup policy: compact
  - Use: Materialized balance output for serving low-latency reads.

- `fraud-alerts`
  - Key: `tenantId|accountId`
  - Value: Avro `FraudAlert`
  - Partitions: 6
  - Use: Downstream alerting, case-management, or risk dashboards.

- `sync.public.sync_runs`
  - Key: Debezium JSON primary-key payload for `public.sync_runs`
  - Value: Debezium JSON envelope for the parent sync run row
  - Partitions: 6
  - Use: CDC source topic for the `sync_runs` table.

- `sync.public.sync_run_payloads`
  - Key: Debezium JSON primary-key payload for `public.sync_run_payloads`
  - Value: Debezium JSON envelope for the child payload row
  - Partitions: 6
  - Use: CDC source topic for the `sync_run_payloads` table.

- `sync-runs-enriched`
  - Key: `sync_run_id`
  - Value: JSON merged view containing the latest `sync_runs` row and the current list of related payload rows
  - Partitions: 6
  - Cleanup policy: compact
  - Use: downstream consumption of a single sync-run-centric document assembled with Kafka Streams.

## Internal State Stores

- `ledger-balance-store`
  - Backing store for account balances per tenant.
  - Queried through interactive queries API.

- `tx-dedup-store` (window store)
  - Remembers `tenantId|accountId|eventId` for `app.ledger.dedup-window` to drop replays.

- `account-currency-store`
  - The currency each account was opened with (its first accepted event).

- `fraud-activity-store`
  - Tracks per-account activity windows for burst detection.
  - Built via the Processor API (`FraudDetectionProcessor`) and queried via REST.

- `fraud-expiry-index-store`
  - `zeroPaddedLastEventTime|accountKey` index so the stream-time punctuator range-scans only
    idle accounts.

- `sync-runs-store`
  - Latest materialized `sync_runs` row per `sync_run_id`.

- `sync-run-payloads-store`
  - Latest payload collection per `sync_run_id`, maintained from Debezium CDC events.

