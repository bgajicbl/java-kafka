# Event Catalog

Complete catalog of all events, commands, state snapshots, and topics in the
Kafka Ledger Learning platform.

---

## Domain Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Finance Streaming Platform                     │
│                                                                     │
│  ┌──────────┐  ┌───────────────┐  ┌──────────┐  ┌──────────────┐  │
│  │  Ledger   │  │    Payment     │  │  Fraud   │  │   Revenue    │  │
│  │ Balances  │  │ Orchestration  │  │Detection │  │  Analytics   │  │
│  └──────────┘  └───────────────┘  └──────────┘  └──────────────┘  │
│                                                                     │
│  ┌──────────────────┐  ┌────────────────────────────────────────┐  │
│  │   CDC Sync Runs   │  │      Dead Letter Queues (DLQ)          │  │
│  └──────────────────┘  └────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Topics

### 1. `tx-events` — Transaction Events

| Property       | Value                              |
|----------------|------------------------------------|
| Schema         | `TransactionEvent` (Avro)          |
| Key            | `tenantId\|accountId`              |
| Partitions     | 6                                  |
| Cleanup        | delete (7-day retention)           |
| Producers      | `TransactionIngestController`      |
| Consumers      | Ledger topology, Fraud topology    |

**Fields:** `eventId`, `tenantId`, `accountId`, `amount`, `direction` (DEBIT/CREDIT), `currency`, `eventTime`

---

### 2. `ledger-balances` — Ledger Balance Snapshots

| Property       | Value                              |
|----------------|------------------------------------|
| Schema         | `LedgerBalance` (Avro)             |
| Key            | `tenantId\|accountId`              |
| Partitions     | 6                                  |
| Cleanup        | compact                            |
| Producers      | Ledger topology (KTable output)    |
| Consumers      | External systems, dashboards       |

**Fields:** `tenantId`, `accountId`, `balance`, `currency`, `lastEventTime`

---

### 3. `fraud-alerts` — Fraud Alert Events

| Property       | Value                              |
|----------------|------------------------------------|
| Schema         | `FraudAlert` (Avro)                |
| Key            | `tenantId\|accountId`              |
| Partitions     | 6                                  |
| Cleanup        | delete (30-day retention)          |
| Producers      | `FraudDetectionProcessor`          |
| Consumers      | Alerting systems, compliance       |

**Fields:** `tenantId`, `accountId`, `eventId`, `reason`, `amount`, `eventTime`

**Alert reasons:**
- `SINGLE_TX_THRESHOLD_EXCEEDED` — single transaction above configured threshold
- `BURST_ACTIVITY_DETECTED` — too many transactions in a time window

---

### 4. `payment-commands` — Payment Lifecycle Commands

| Property       | Value                              |
|----------------|------------------------------------|
| Schema         | `PaymentEvent` (Avro)              |
| Key            | `tenantId\|paymentId`              |
| Partitions     | 6                                  |
| Cleanup        | delete (7-day retention)           |
| Producers      | `PaymentController`                |
| Consumers      | Payment orchestration topology     |

**Fields:** `paymentId`, `tenantId`, `accountId`, `amount`, `currency`, `commandType`, `correlationId`, `eventTime`

**Command types:** `INITIATE`, `AUTHORIZE`, `CAPTURE`, `SETTLE`, `REVERSE`

---

### 5. `payment-events` — Successful Payment State Changes

| Property       | Value                              |
|----------------|------------------------------------|
| Schema         | `PaymentState` (JSON)              |
| Key            | `tenantId\|paymentId`              |
| Partitions     | 6                                  |
| Cleanup        | compact                            |
| Producers      | Payment orchestration topology     |
| Consumers      | Windowed join, Revenue aggregation |

**State machine:**
```
UNKNOWN ──INITIATE──▶ INITIATED
                          │
                     AUTHORIZE
                          ▼
                      AUTHORIZED ──REVERSE──▶ REVERSED
                          │
                       CAPTURE
                          ▼
                      CAPTURED ──REVERSE──▶ REVERSED
                          │
                       SETTLE
                          ▼
                      SETTLED (terminal)
```

---

### 6. `payment-dlq` — Failed Payment Transitions

| Property       | Value                              |
|----------------|------------------------------------|
| Schema         | `PaymentState` (JSON)              |
| Key            | `tenantId\|paymentId`              |
| Partitions     | 3                                  |
| Cleanup        | delete (30-day retention)          |
| Producers      | Payment orchestration topology     |
| Consumers      | Ops monitoring, manual review      |

Records where `failReason != null` — the status is unchanged, but the
`failReason` field describes why the command was rejected.

---

### 7. `payment-matched` — Windowed Join Results

| Property       | Value                              |
|----------------|------------------------------------|
| Schema         | JSON string                        |
| Key            | `tenantId\|paymentId`              |
| Partitions     | 6                                  |
| Cleanup        | delete (7-day retention)           |
| Producers      | Windowed join topology             |
| Consumers      | Reconciliation systems             |

INITIATED ↔ CAPTURED matches within a 10-minute window, with
`processingTimeMs` measuring the lifecycle duration.

---

### 8. `tenant-revenue-hourly` — Hourly Revenue Aggregates

| Property       | Value                              |
|----------------|------------------------------------|
| Schema         | `TenantRevenue` (Avro)             |
| Key            | `tenantId`                         |
| Partitions     | 6                                  |
| Cleanup        | delete (90-day retention)          |
| Producers      | Revenue aggregation topology       |
| Consumers      | Finance dashboards, reporting      |

**Fields:** `tenantId`, `windowStart`, `windowEnd`, `totalAmount`, `transactionCount`, `currency`

---

### 9. CDC Topics (Debezium)

| Topic                         | Source Table              | Key             |
|-------------------------------|---------------------------|-----------------|
| `sync.public.sync_runs`       | `public.sync_runs`        | `sync_run_id`   |
| `sync.public.sync_run_payloads`| `public.sync_run_payloads`| `sync_run_payload_id` |
| `sync-runs-enriched`          | Merged KTable output      | `sync_run_id`   |

---

### 10. Dead Letter Queues

| Topic                         | Purpose                            |
|-------------------------------|------------------------------------|
| `dlq-deserialization-errors`  | Poison pills that failed to deserialise |
| `dlq-processing-errors`       | Records that caused processing exceptions |
| `payment-dlq`                 | Invalid payment state transitions   |

---

## State Stores (RocksDB)

| Store Name              | Type           | Key Format              | Value Type                |
|-------------------------|----------------|-------------------------|---------------------------|
| `ledger-balance-store`  | KeyValue       | `tenantId\|accountId`   | `LedgerBalance` (Avro)    |
| `fraud-activity-store`  | KeyValue       | `tenantId\|accountId`   | `AccountActivity` (JSON)  |
| `payment-state-store`   | KeyValue       | `tenantId\|paymentId`   | `PaymentState` (JSON)     |
| `revenue-hourly-store`  | WindowStore    | `tenantId` (windowed)   | `TenantRevenue` (Avro)    |
| `sync-runs-store`       | KeyValue       | `sync_run_id`           | `SyncRunRecord` (JSON)    |
| `sync-run-payloads-store`| KeyValue      | `sync_run_id`           | `SyncRunPayloadAggregate` |
| `sync-runs-merged-store`| KeyValue       | `sync_run_id`           | `SyncRunMergedRecord`     |

---

## REST API Endpoints (Interactive Queries)

| Method | Path                                    | State Store            | Description                          |
|--------|-----------------------------------------|------------------------|--------------------------------------|
| GET    | `/api/ledger/{tenantId}/{accountId}`    | ledger-balance-store   | Current balance for an account       |
| GET    | `/api/fraud/{tenantId}/{accountId}`     | fraud-activity-store   | Fraud activity window state          |
| GET    | `/api/payments/{tenantId}/{paymentId}`  | payment-state-store    | Current payment lifecycle state      |
| GET    | `/api/revenue/{tenantId}?from=&to=`     | revenue-hourly-store   | Hourly revenue windows in time range |
| GET    | `/api/sync-runs/{syncRunId}`            | sync-runs-merged-store | CDC-merged sync run with payloads    |
| POST   | `/api/transactions`                     | —                      | Submit a transaction event           |
| POST   | `/api/payments`                         | —                      | Submit a payment command             |

---

## Key Design Decisions

### Multi-Tenant Key Convention
All keys follow `tenantId|subKey` format, enabling the `TenantAwarePartitioner`
to hash only on `tenantId`, ensuring co-partitioned tenant data.

### Exactly-Once Semantics (EOS)
Configured with `processing.guarantee: exactly_once_v2` — the strongest
consistency guarantee available in Kafka Streams 3.x.

### Error Handling Strategy
- **Deserialization errors** → `StreamsDeserializationErrorHandler` logs and skips
- **Production errors** → `StreamsProductionExceptionHandler` logs and continues
- **Invalid business transitions** → routed to domain-specific DLQ topics
- **DLQ headers** include `dlq.original.topic`, `dlq.error.message`, `dlq.error.stacktrace`

### Serialization Strategy
- **Avro** for core financial events (schema evolution, Schema Registry)
- **JSON** for internal state stores and CDC processing (flexibility)
- **String** for composite keys (tenantId|subKey pattern)

