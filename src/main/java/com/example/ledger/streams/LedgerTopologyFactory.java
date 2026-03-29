package com.example.ledger.streams;

import com.example.ledger.avro.FraudAlert;
import com.example.ledger.avro.LedgerBalance;
import com.example.ledger.avro.TransactionEvent;
import com.example.ledger.avro.TxDirection;
import com.example.ledger.config.LedgerStreamProperties;
import com.example.ledger.model.AccountActivity;
import com.example.ledger.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public final class LedgerTopologyFactory {

    private LedgerTopologyFactory() {
    }

    public static KStream<String, TransactionEvent> build(StreamsBuilder builder,
                                                           LedgerStreamProperties properties,
                                                           Serde<TransactionEvent> transactionSerde,
                                                           Serde<LedgerBalance> ledgerBalanceSerde,
                                                           Serde<FraudAlert> fraudAlertSerde) {
        Serde<String> stringSerde = Serdes.String();

        StoreBuilder<KeyValueStore<String, AccountActivity>> activityStoreBuilder = Stores
                .keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(Topics.FRAUD_ACTIVITY_STORE),
                        stringSerde,
                        new JsonSerde<>(AccountActivity.class)
                );
        builder.addStateStore(activityStoreBuilder);

        KStream<String, TransactionEvent> events = builder
                .stream(Topics.TRANSACTION_EVENTS, Consumed.with(stringSerde, transactionSerde))
                .selectKey((key, value) -> value.getTenantId() + "|" + value.getAccountId());

        KGroupedStream<String, TransactionEvent> groupedEvents = events.groupByKey(Grouped.with(stringSerde, transactionSerde));

        KTable<String, LedgerBalance> balances = groupedEvents.aggregate(
                () -> LedgerBalance.newBuilder()
                        .setTenantId("unknown")
                        .setAccountId("unknown")
                        .setBalance(0.0)
                        .setCurrency("USD")
                        .setLastEventTime(0L)
                        .build(),
                (key, event, aggregate) -> {
                    String[] parts = key.split("\\|", 2);
                    double signedAmount = event.getDirection() == TxDirection.DEBIT
                            ? -Math.abs(event.getAmount())
                            : Math.abs(event.getAmount());
                    return LedgerBalance.newBuilder()
                            .setTenantId(parts[0])
                            .setAccountId(parts.length > 1 ? parts[1] : "unknown")
                            .setBalance(aggregate.getBalance() + signedAmount)
                            .setCurrency(event.getCurrency())
                            .setLastEventTime(event.getEventTime())
                            .build();
                },
                Materialized.<String, LedgerBalance, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(Topics.BALANCE_STORE)
                        .withKeySerde(stringSerde)
                        .withValueSerde(ledgerBalanceSerde)
        );

        balances.toStream().to(Topics.LEDGER_BALANCES, Produced.with(stringSerde, ledgerBalanceSerde));

        events.<FraudAlert>processValues(
                        () -> new FraudDetectionProcessor(properties),
                        Named.as("fraud-detector"),
                        Topics.FRAUD_ACTIVITY_STORE)
                .filter((key, alert) -> alert != null)
                .to(Topics.FRAUD_ALERTS, Produced.with(stringSerde, fraudAlertSerde));

        return events;
    }
}
