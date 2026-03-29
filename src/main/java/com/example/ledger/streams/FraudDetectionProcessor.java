package com.example.ledger.streams;

import com.example.ledger.avro.FraudAlert;
import com.example.ledger.avro.TransactionEvent;
import com.example.ledger.config.LedgerStreamProperties;
import com.example.ledger.model.AccountActivity;
import java.time.Duration;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Modern Processor API implementation (Kafka 3.3+) replacing the deprecated {@code Transformer}.
 *
 * <p>This {@link FixedKeyProcessor} demonstrates:
 * <ul>
 *   <li>Native Processor API with typed context and records</li>
 *   <li>Direct state store access for fraud-activity tracking</li>
 *   <li>Wall-clock {@link org.apache.kafka.streams.processor.Punctuator} for scheduled
 *       cleanup of stale activity windows (memory management)</li>
 *   <li>Conditional forwarding — only emits when a fraud alert is detected</li>
 * </ul>
 *
 * <p>Used via {@code KStream.processValues()} in the DSL, which preserves the
 * record key while allowing full Processor API capabilities.
 */
public class FraudDetectionProcessor
        implements FixedKeyProcessor<String, TransactionEvent, FraudAlert> {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionProcessor.class);

    private final LedgerStreamProperties properties;
    private FixedKeyProcessorContext<String, FraudAlert> context;
    private KeyValueStore<String, AccountActivity> activityStore;

    public FraudDetectionProcessor(LedgerStreamProperties properties) {
        this.properties = properties;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, FraudAlert> context) {
        this.context = context;
        this.activityStore = context.getStateStore(Topics.FRAUD_ACTIVITY_STORE);

        // ── Punctuator ────────────────────────────────────────────────
        // Runs every 30 seconds on wall-clock time to evict stale
        // activity windows from the state store, preventing unbounded
        // state growth — a critical production concern with RocksDB.
        context.schedule(
                Duration.ofSeconds(30),
                PunctuationType.WALL_CLOCK_TIME,
                this::expireStaleWindows
        );

        log.info("FraudDetectionProcessor initialised – burst window {}ms, threshold {}",
                properties.getBurstWindowMs(), properties.getBurstCountThreshold());
    }

    @Override
    public void process(FixedKeyRecord<String, TransactionEvent> record) {
        String key = record.key();
        TransactionEvent value = record.value();
        if (value == null) {
            return;
        }

        // ── Update activity state ────────────────────────────────────
        AccountActivity activity = activityStore.get(key);
        if (activity == null) {
            activity = new AccountActivity();
            activity.setWindowStartMs(value.getEventTime());
        }

        if (value.getEventTime() - activity.getWindowStartMs() > properties.getBurstWindowMs()) {
            activity.setWindowStartMs(value.getEventTime());
            activity.setEventsInWindow(0);
        }

        activity.setEventsInWindow(activity.getEventsInWindow() + 1);
        activity.setLastEventTime(value.getEventTime());
        activityStore.put(key, activity);

        // ── Evaluate fraud rules ─────────────────────────────────────
        double absoluteAmount = Math.abs(value.getAmount());
        boolean amountAlert = absoluteAmount >= properties.getSingleTransactionAlertThreshold();
        boolean burstAlert = activity.getEventsInWindow() >= properties.getBurstCountThreshold();

        if (!amountAlert && !burstAlert) {
            return; // No alert — nothing forwarded downstream
        }

        String[] parts = key.split("\\|", 2);
        String reason = amountAlert
                ? "SINGLE_TX_THRESHOLD_EXCEEDED"
                : "BURST_ACTIVITY_DETECTED";

        FraudAlert alert = FraudAlert.newBuilder()
                .setTenantId(parts[0])
                .setAccountId(parts.length > 1 ? parts[1] : "unknown")
                .setEventId(value.getEventId())
                .setReason(reason)
                .setAmount(value.getAmount())
                .setEventTime(value.getEventTime())
                .build();

        // Forward the alert downstream (key is preserved by FixedKeyProcessor)
        context.forward(record.withValue(alert));
    }

    /**
     * Punctuator callback: evicts entries whose last event is older than
     * twice the burst window, preventing unbounded state store growth.
     */
    private void expireStaleWindows(long wallClockTimestamp) {
        long staleThresholdMs = properties.getBurstWindowMs() * 2;
        int evicted = 0;

        try (KeyValueIterator<String, AccountActivity> iter = activityStore.all()) {
            while (iter.hasNext()) {
                KeyValue<String, AccountActivity> entry = iter.next();
                long age = wallClockTimestamp - entry.value.getLastEventTime();
                if (age > staleThresholdMs) {
                    activityStore.delete(entry.key);
                    evicted++;
                }
            }
        }

        if (evicted > 0) {
            log.debug("Punctuator evicted {} stale activity entries", evicted);
        }
    }

    @Override
    public void close() {
        // Nothing to release
    }
}

