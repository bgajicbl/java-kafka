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

All monetary fields on every topic are Avro `decimal(19,4)` (`BigDecimal`), never floating point.

**Processing guarantees:** events are repartitioned by `tenantId|accountId`, then a guard drops
replays of the same `eventId` within `app.ledger.dedup-window` (default 7 days) and rejects
events whose currency differs from the account's first event (→ `ledger-dlq`).

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
| Key            | `tenantId\|currency`               |
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
| `dlq-deserialization-errors`  | Poison pills that failed to deserialise — original bytes + `dlq.original.*` / `dlq.error.*` headers |
| `payment-dlq`                 | Invalid payment state transitions   |
| `ledger-dlq`                  | Transactions rejected by the ledger guard (`TransactionEvent`; reason in `dlq.reason` header) |

---

## State Stores (RocksDB)

| Store Name              | Type           | Key Format              | Value Type                |
|-------------------------|----------------|-------------------------|---------------------------|
| `ledger-balance-store`  | KeyValue       | `tenantId\|accountId`   | `LedgerBalance` (Avro)    |
| `fraud-activity-store`  | KeyValue       | `tenantId\|accountId`   | `AccountActivity` (JSON)  |
| `fraud-expiry-index-store` | KeyValue    | `paddedLastEventTime\|tenantId\|accountId` | account key (String) |
| `payment-state-store`   | KeyValue       | `tenantId\|paymentId`   | `PaymentState` (JSON)     |
| `revenue-hourly-store`  | WindowStore    | `tenantId\|currency` (windowed) | `TenantRevenue` (Avro) |
| `tx-dedup-store`        | WindowStore    | `tenantId\|accountId\|eventId` | event timestamp (Long) |
| `account-currency-store`| KeyValue       | `tenantId\|accountId`   | ISO-4217 code (String)    |
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
| GET    | `/api/revenue/{tenantId}?currency=&from=&to=` | revenue-hourly-store | Hourly revenue windows for one currency |
| GET    | `/api/sync-runs/{syncRunId}`            | sync-runs-merged-store | CDC-merged sync run with payloads    |
| POST   | `/api/transactions`                     | —                      | Submit a transaction event           |
| POST   | `/api/payments`                         | —                      | Submit a payment command             |

---

## Key Design Decisions

### Multi-Tenant Key Convention
All keys follow `tenantId|subKey` format and are hashed with the default partitioner on the
full key. Hashing on `tenantId` alone was considered and rejected: one large tenant would pin
a single partition (and a single stream thread), and no downstream consumer needs cross-account
ordering within a tenant. IDs are validated (`[A-Za-z0-9._-]{1,64}`) so no component can contain
the `|` delimiter, and tenant isolation is enforced at the API by the JWT `tenant_id` claim.

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

