package com.example.ledger.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Exposes custom business metrics via Micrometer for Prometheus scraping.
 *
 * <p>Counters are registered eagerly so that Prometheus sees them immediately
 * (even before the first increment). Application code increments them via
 * {@code registry.counter("metric.name").increment()}.
 *
 * <p>Metrics exposed:
 * <ul>
 *   <li>{@code ledger_transactions_processed_total} — transactions through the ledger topology</li>
 *   <li>{@code payments_state_transitions_total} — payment state-machine transitions</li>
 *   <li>{@code fraud_alerts_emitted_total} — fraud alerts generated</li>
 *   <li>{@code dlq_records_total} — records routed to any DLQ topic</li>
 * </ul>
 */
@Configuration
public class MetricsConfig {

    @Bean
    public MeterBinder customKafkaStreamMetrics() {
        return registry -> {
            Counter.builder("ledger.transactions.processed")
                    .description("Total transactions processed by the ledger topology")
                    .register(registry);

            Counter.builder("payments.state.transitions")
                    .description("Total payment state-machine transitions")
                    .register(registry);

            Counter.builder("fraud.alerts.emitted")
                    .description("Total fraud alerts emitted")
                    .register(registry);

            Counter.builder("dlq.records")
                    .description("Total records routed to dead-letter queues")
                    .register(registry);
        };
    }
}

