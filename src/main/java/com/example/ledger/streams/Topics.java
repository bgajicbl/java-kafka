package com.example.ledger.streams;

public final class Topics {

    // ── Ledger & Fraud ───────────────────────────────────────────────
    public static final String TRANSACTION_EVENTS = "tx-events";
    public static final String LEDGER_BALANCES = "ledger-balances";
    public static final String FRAUD_ALERTS = "fraud-alerts";
    public static final String LEDGER_DLQ = "ledger-dlq";

    // ── CDC (Debezium) ───────────────────────────────────────────────
    public static final String SYNC_RUNS_CDC = "sync.public.sync_runs";
    public static final String SYNC_RUN_PAYLOADS_CDC = "sync.public.sync_run_payloads";
    public static final String SYNC_RUNS_ENRICHED = "sync-runs-enriched";

    // ── Payment Orchestration ────────────────────────────────────────
    public static final String PAYMENT_COMMANDS = "payment-commands";
    public static final String PAYMENT_EVENTS = "payment-events";
    public static final String PAYMENT_DLQ = "payment-dlq";
    public static final String PAYMENT_MATCHED = "payment-matched";

    // ── Revenue Analytics ────────────────────────────────────────────
    public static final String TENANT_REVENUE_HOURLY = "tenant-revenue-hourly";

    // ── Dead Letter Queues ───────────────────────────────────────────
    public static final String DLQ_DESERIALIZATION = "dlq-deserialization-errors";

    // ── State Stores ─────────────────────────────────────────────────
    public static final String BALANCE_STORE = "ledger-balance-store";
    public static final String FRAUD_ACTIVITY_STORE = "fraud-activity-store";
    public static final String FRAUD_EXPIRY_INDEX_STORE = "fraud-expiry-index-store";
    public static final String TX_DEDUP_STORE = "tx-dedup-store";
    public static final String ACCOUNT_CURRENCY_STORE = "account-currency-store";
    public static final String SYNC_RUNS_STORE = "sync-runs-store";
    public static final String SYNC_RUN_PAYLOADS_STORE = "sync-run-payloads-store";
    public static final String SYNC_RUNS_MERGED_STORE = "sync-runs-merged-store";
    public static final String PAYMENT_STATE_STORE = "payment-state-store";
    public static final String REVENUE_STORE = "revenue-hourly-store";

    private Topics() {
    }
}

