# Topic Design

## Core Topics

- `tx-events`
  - Key: `tenantId|accountId`
  - Value: Avro `TransactionEvent`
  - Partitions: 6
  - Use: Source-of-truth input stream for payment and ledger mutations.

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

- `fraud-activity-store`
  - Tracks per-account activity windows for burst detection.
  - Built via Processor API transformer and queried via REST.

- `sync-runs-store`
  - Latest materialized `sync_runs` row per `sync_run_id`.

- `sync-run-payloads-store`
  - Latest payload collection per `sync_run_id`, maintained from Debezium CDC events.

