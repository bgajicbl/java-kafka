package com.example.ledger.streams;

import com.example.ledger.avro.TransactionEvent;
import com.example.ledger.config.LedgerMetrics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ingress guard for the ledger: runs before anything touches a balance.
 *
 * <ul>
 *   <li><b>Idempotency</b> — EOS only protects Kafka-internal read-process-write cycles; a client
 *       that retries an HTTP POST produces a second, distinct record. Each {@code eventId} (scoped
 *       to its {@code tenantId|accountId} key, since IDs are tenant-supplied) is remembered in a
 *       window store for {@code dedupWindow} and replays are dropped.</li>
 *   <li><b>Single-currency accounts</b> — the first accepted event pins the account's currency;
 *       later events in another currency are rejected rather than summed into the balance.</li>
 * </ul>
 *
 * <p>Must run on a stream already partitioned by {@code tenantId|accountId}, so that every
 * event for an account — and every replay of it — reaches the same task and store.
 */
public class TransactionGuardProcessor
        implements FixedKeyProcessor<String, TransactionEvent, TransactionGuardProcessor.Verdict> {

    /** Header carrying the rejection reason on records routed to {@link Topics#LEDGER_DLQ}. */
    public static final String REJECT_REASON_HEADER = "dlq.reason";

    private static final Logger log = LoggerFactory.getLogger(TransactionGuardProcessor.class);

    private final long dedupWindowMs;
    private final LedgerMetrics metrics;
    private FixedKeyProcessorContext<String, Verdict> context;
    private WindowStore<String, Long> dedupStore;
    private KeyValueStore<String, String> currencyStore;

    public TransactionGuardProcessor(Duration dedupWindow, LedgerMetrics metrics) {
        this.dedupWindowMs = dedupWindow.toMillis();
        this.metrics = metrics;
    }

    /** Outcome of the guard; {@code rejectReason == null} means the event may be applied. */
    public record Verdict(TransactionEvent event, String rejectReason) {
        public boolean accepted() {
            return rejectReason == null;
        }
    }

    @Override
    public void init(FixedKeyProcessorContext<String, Verdict> context) {
        this.context = context;
        this.dedupStore = context.getStateStore(Topics.TX_DEDUP_STORE);
        this.currencyStore = context.getStateStore(Topics.ACCOUNT_CURRENCY_STORE);
    }

    @Override
    public void process(FixedKeyRecord<String, TransactionEvent> record) {
        TransactionEvent event = record.value();
        if (event == null) {
            return;
        }

        String dedupKey = record.key() + "|" + event.getEventId();
        long timestamp = record.timestamp();
        if (isDuplicate(dedupKey, timestamp)) {
            log.debug("Dropping duplicate event {} for {}", event.getEventId(), record.key());
            metrics.duplicateDropped();
            return;
        }
        dedupStore.put(dedupKey, timestamp, timestamp);

        String accountCurrency = currencyStore.get(record.key());
        if (accountCurrency == null) {
            currencyStore.put(record.key(), event.getCurrency());
        } else if (!accountCurrency.equals(event.getCurrency())) {
            String reason = "CURRENCY_MISMATCH: account is " + accountCurrency + ", event is " + event.getCurrency();
            record.headers().add(REJECT_REASON_HEADER, reason.getBytes(StandardCharsets.UTF_8));
            context.forward(record.withValue(new Verdict(event, reason)));
            return;
        }

        context.forward(record.withValue(new Verdict(event, null)));
    }

    private boolean isDuplicate(String dedupKey, long timestamp) {
        try (WindowStoreIterator<Long> seen = dedupStore.fetch(
                dedupKey, timestamp - dedupWindowMs, timestamp + dedupWindowMs)) {
            return seen.hasNext();
        }
    }
}
