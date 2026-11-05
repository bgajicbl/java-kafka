package com.example.ledger.streams;

import com.example.ledger.avro.FraudAlert;
import com.example.ledger.avro.TransactionEvent;
import com.example.ledger.config.LedgerStreamProperties;
import com.example.ledger.model.AccountActivity;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
 * Processor API implementation of the burst / single-amount fraud rules.
 *
 * <p>This {@link FixedKeyProcessor} demonstrates:
 * <ul>
 *   <li>Native Processor API with typed context and records</li>
 *   <li>Direct state store access for fraud-activity tracking</li>
 *   <li>A <b>stream-time</b> {@link org.apache.kafka.streams.processor.Punctuator} that evicts
 *       idle accounts. Activity timestamps are event time, so eviction must be driven by event
 *       time too: comparing them against the wall clock would evict everything immediately when
 *       replaying history, and never evict under a backlog.</li>
 *   <li>A secondary <b>expiry index</b> store keyed by {@code paddedLastEventTime|accountKey}, so
 *       each punctuation range-scans only the expired prefix instead of every account.</li>
 *   <li>Conditional forwarding — only emits when a fraud alert is detected</li>
 * </ul>
 *
 * <p>Used via {@code KStream.processValues()} in the DSL, which preserves the
 * record key while allowing full Processor API capabilities.
 */
public class FraudDetectionProcessor
        implements FixedKeyProcessor<String, TransactionEvent, FraudAlert> {

    static final Duration PUNCTUATE_INTERVAL = Duration.ofSeconds(30);

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionProcessor.class);

    private final LedgerStreamProperties properties;
    private FixedKeyProcessorContext<String, FraudAlert> context;
    private KeyValueStore<String, AccountActivity> activityStore;
    private KeyValueStore<String, String> expiryIndex;

    public FraudDetectionProcessor(LedgerStreamProperties properties) {
        this.properties = properties;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, FraudAlert> context) {
        this.context = context;
        this.activityStore = context.getStateStore(Topics.FRAUD_ACTIVITY_STORE);
        this.expiryIndex = context.getStateStore(Topics.FRAUD_EXPIRY_INDEX_STORE);

        context.schedule(PUNCTUATE_INTERVAL, PunctuationType.STREAM_TIME, this::expireIdleAccounts);

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
        long eventTime = record.timestamp();

        // ── Update activity state ────────────────────────────────────
        AccountActivity activity = activityStore.get(key);
        if (activity == null) {
            activity = new AccountActivity();
            activity.setWindowStartMs(eventTime);
        } else {
            expiryIndex.delete(indexKey(activity.getLastEventTime(), key));
        }

        if (eventTime - activity.getWindowStartMs() > properties.getBurstWindowMs()) {
            activity.setWindowStartMs(eventTime);
            activity.setEventsInWindow(0);
        }

        activity.setEventsInWindow(activity.getEventsInWindow() + 1);
        // Out-of-order events must not move the idle clock backwards
        activity.setLastEventTime(Math.max(activity.getLastEventTime(), eventTime));
        activityStore.put(key, activity);
        expiryIndex.put(indexKey(activity.getLastEventTime(), key), key);

        // ── Evaluate fraud rules ─────────────────────────────────────
        boolean amountAlert = value.getAmount().abs().compareTo(properties.getSingleTransactionAlertThreshold()) >= 0;
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
     * Punctuator callback: evicts accounts idle for more than twice the burst window
     * (in stream time). Only index entries older than the cutoff are visited.
     */
    private void expireIdleAccounts(long streamTime) {
        long cutoff = streamTime - properties.getBurstWindowMs() * 2;
        if (cutoff <= 0) {
            return;
        }

        List<KeyValue<String, String>> expired = new ArrayList<>();
        // Upper bound "<cutoff>|" sorts before every "<cutoff>|<key>", so only strictly older entries match
        try (KeyValueIterator<String, String> iter = expiryIndex.range(indexKey(0, ""), indexKey(cutoff, ""))) {
            iter.forEachRemaining(expired::add);
        }
        for (KeyValue<String, String> entry : expired) {
            expiryIndex.delete(entry.key);
            activityStore.delete(entry.value);
        }

        if (!expired.isEmpty()) {
            log.debug("Punctuator evicted {} idle activity entries", expired.size());
        }
    }

    /** Zero-padded so lexicographic order of the string key equals numeric order of the time. */
    static String indexKey(long lastEventTime, String accountKey) {
        return String.format("%019d|%s", lastEventTime, accountKey);
    }
}
