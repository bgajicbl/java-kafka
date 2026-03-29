package com.example.ledger.streams;

import com.example.ledger.avro.FraudAlert;
import com.example.ledger.avro.TransactionEvent;
import com.example.ledger.config.LedgerStreamProperties;
import com.example.ledger.model.AccountActivity;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class FraudDetectorTransformer implements Transformer<String, TransactionEvent, KeyValue<String, FraudAlert>> {

    private final LedgerStreamProperties properties;
    private KeyValueStore<String, AccountActivity> activityStore;

    public FraudDetectorTransformer(LedgerStreamProperties properties) {
        this.properties = properties;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        activityStore = (KeyValueStore<String, AccountActivity>) context.getStateStore(Topics.FRAUD_ACTIVITY_STORE);
    }

    @Override
    public KeyValue<String, FraudAlert> transform(String key, TransactionEvent value) {
        if (value == null) {
            return null;
        }

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

        double absoluteAmount = Math.abs(value.getAmount());
        boolean amountAlert = absoluteAmount >= properties.getSingleTransactionAlertThreshold();
        boolean burstAlert = activity.getEventsInWindow() >= properties.getBurstCountThreshold();

        if (!amountAlert && !burstAlert) {
            return null;
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

        return KeyValue.pair(key, alert);
    }

    @Override
    public void close() {
    }
}

