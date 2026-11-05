package com.example.ledger.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Business metrics for the streaming topologies, exported to Prometheus as
 * {@code ledger_transactions_processed_total}, {@code ledger_transactions_duplicates_total},
 * {@code payments_state_transitions_total{status}}, {@code fraud_alerts_emitted_total{reason}}
 * and {@code dlq_records_total{topic}}.
 *
 * <p>Passed explicitly into the topology factories (rather than looked up globally) so that
 * topology tests can assert on them with a {@code SimpleMeterRegistry}.
 *
 * <p>Counters are incremented from stream threads. Under exactly-once, a task that aborts
 * and retries a transaction may count the same record twice — these are operational
 * signals, not reconciliation figures.
 */
@Component
public class LedgerMetrics {

    private final MeterRegistry registry;
    private final Counter transactionsProcessed;
    private final Counter duplicatesDropped;

    public LedgerMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.transactionsProcessed = Counter.builder("ledger.transactions.processed")
                .description("Transactions applied to a ledger balance")
                .register(registry);
        this.duplicatesDropped = Counter.builder("ledger.transactions.duplicates")
                .description("Replayed transactions dropped by eventId de-duplication")
                .register(registry);
    }

    public void transactionProcessed() {
        transactionsProcessed.increment();
    }

    public void duplicateDropped() {
        duplicatesDropped.increment();
    }

    public void paymentTransition(String status) {
        registry.counter("payments.state.transitions", "status", status).increment();
    }

    public void fraudAlert(String reason) {
        registry.counter("fraud.alerts.emitted", "reason", reason).increment();
    }

    public void deadLettered(String topic) {
        registry.counter("dlq.records", "topic", topic).increment();
    }
}
