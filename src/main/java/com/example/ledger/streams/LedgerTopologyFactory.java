package com.example.ledger.streams;

import com.example.ledger.avro.FraudAlert;
import com.example.ledger.avro.LedgerBalance;
import com.example.ledger.avro.TransactionEvent;
import com.example.ledger.avro.TxDirection;
import com.example.ledger.config.LedgerMetrics;
import com.example.ledger.config.LedgerStreamProperties;
import com.example.ledger.model.AccountActivity;
import com.example.ledger.model.Money;
import com.example.ledger.serde.JsonSerde;
import java.math.BigDecimal;
import java.time.Duration;
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
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public final class LedgerTopologyFactory {

    private LedgerTopologyFactory() {
    }

    public static KStream<String, TransactionEvent> build(StreamsBuilder builder,
                                                           LedgerStreamProperties properties,
                                                           LedgerMetrics metrics,
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
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Topics.FRAUD_EXPIRY_INDEX_STORE),
                stringSerde,
                stringSerde));

        Duration dedupWindow = properties.getDedupWindow();
        builder.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore(Topics.TX_DEDUP_STORE, dedupWindow, dedupWindow, false),
                stringSerde,
                Serdes.Long()));
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Topics.ACCOUNT_CURRENCY_STORE),
                stringSerde,
                stringSerde));

        // Explicit repartition so the guard below sees every event for an account on one task,
        // regardless of how upstream producers keyed tx-events.
        KStream<String, TransactionEvent> events = builder
                .stream(Topics.TRANSACTION_EVENTS, Consumed.with(stringSerde, transactionSerde))
                .selectKey((key, value) -> value.getTenantId() + "|" + value.getAccountId())
                .repartition(Repartitioned.with(stringSerde, transactionSerde).withName("tx-by-account"));

        KStream<String, TransactionGuardProcessor.Verdict> verdicts = events.processValues(
                () -> new TransactionGuardProcessor(dedupWindow, metrics),
                Named.as("tx-guard"),
                Topics.TX_DEDUP_STORE, Topics.ACCOUNT_CURRENCY_STORE);

        verdicts
                .filter((key, verdict) -> !verdict.accepted(), Named.as("tx-rejected-filter"))
                .mapValues(TransactionGuardProcessor.Verdict::event)
                .peek((key, event) -> metrics.deadLettered(Topics.LEDGER_DLQ))
                .to(Topics.LEDGER_DLQ, Produced.with(stringSerde, transactionSerde));

        KStream<String, TransactionEvent> accepted = verdicts
                .filter((key, verdict) -> verdict.accepted(), Named.as("tx-accepted-filter"))
                .mapValues(TransactionGuardProcessor.Verdict::event)
                .peek((key, event) -> metrics.transactionProcessed());

        KGroupedStream<String, TransactionEvent> groupedEvents = accepted.groupByKey(Grouped.with(stringSerde, transactionSerde));

        KTable<String, LedgerBalance> balances = groupedEvents.aggregate(
                () -> LedgerBalance.newBuilder()
                        .setTenantId("unknown")
                        .setAccountId("unknown")
                        .setBalance(Money.ZERO)
                        .setCurrency(Money.DEFAULT_CURRENCY)
                        .setLastEventTime(0L)
                        .build(),
                (key, event, aggregate) -> {
                    String[] parts = key.split("\\|", 2);
                    BigDecimal signedAmount = event.getDirection() == TxDirection.DEBIT
                            ? event.getAmount().abs().negate()
                            : event.getAmount().abs();
                    return LedgerBalance.newBuilder()
                            .setTenantId(parts[0])
                            .setAccountId(parts.length > 1 ? parts[1] : "unknown")
                            .setBalance(Money.normalize(aggregate.getBalance().add(signedAmount)))
                            .setCurrency(event.getCurrency())
                            .setLastEventTime(event.getEventTime())
                            .build();
                },
                Materialized.<String, LedgerBalance, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(Topics.BALANCE_STORE)
                        .withKeySerde(stringSerde)
                        .withValueSerde(ledgerBalanceSerde)
        );

        balances.toStream().to(Topics.LEDGER_BALANCES, Produced.with(stringSerde, ledgerBalanceSerde));

        accepted.<FraudAlert>processValues(
                        () -> new FraudDetectionProcessor(properties),
                        Named.as("fraud-detector"),
                        Topics.FRAUD_ACTIVITY_STORE, Topics.FRAUD_EXPIRY_INDEX_STORE)
                .filter((key, alert) -> alert != null)
                .peek((key, alert) -> metrics.fraudAlert(alert.getReason()))
                .to(Topics.FRAUD_ALERTS, Produced.with(stringSerde, fraudAlertSerde));

        return accepted;
    }
}
