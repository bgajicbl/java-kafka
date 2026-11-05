package com.example.ledger.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.ledger.avro.FraudAlert;
import com.example.ledger.avro.LedgerBalance;
import com.example.ledger.avro.TransactionEvent;
import com.example.ledger.avro.TxDirection;
import com.example.ledger.config.LedgerMetrics;
import com.example.ledger.config.LedgerStreamProperties;
import com.example.ledger.model.AccountActivity;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

class LedgerTopologyTest {

    private static final String MOCK_SCHEMA_REGISTRY = "mock://ledger-topology-test";

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    @Test
    void aggregatesBalancesPerTenantAndAccount() {
        try (TopologyTestDriver driver = createDriver()) {
            TestInputTopic<String, TransactionEvent> input = inputTopic(driver);
            TestOutputTopic<String, LedgerBalance> balances = balancesTopic(driver);

            input.pipeInput("ignored", event("e1", "tenant-a", "acct-1", "1000", TxDirection.CREDIT, "USD", 1_000L));
            input.pipeInput("ignored", event("e2", "tenant-a", "acct-1", "250", TxDirection.DEBIT, "USD", 2_000L));

            List<LedgerBalance> outputs = balances.readValuesToList();
            assertThat(outputs).hasSize(2);
            LedgerBalance latest = outputs.get(1);

            assertThat(latest.getTenantId()).isEqualTo("tenant-a");
            assertThat(latest.getAccountId()).isEqualTo("acct-1");
            assertThat(latest.getBalance()).isEqualByComparingTo("750");
        }
    }

    @Test
    void keepsExactDecimalArithmetic() {
        try (TopologyTestDriver driver = createDriver()) {
            TestInputTopic<String, TransactionEvent> input = inputTopic(driver);
            TestOutputTopic<String, LedgerBalance> balances = balancesTopic(driver);

            // 0.1 + 0.2 is 0.30000000000000004 in binary floating point
            input.pipeInput("ignored", event("e1", "tenant-a", "acct-1", "0.1", TxDirection.CREDIT, "USD", 1_000L));
            input.pipeInput("ignored", event("e2", "tenant-a", "acct-1", "0.2", TxDirection.CREDIT, "USD", 2_000L));

            List<LedgerBalance> outputs = balances.readValuesToList();
            assertThat(outputs.get(1).getBalance()).isEqualTo(new BigDecimal("0.3000"));
        }
    }

    @Test
    void ignoresReplayedEventId() {
        try (TopologyTestDriver driver = createDriver()) {
            TestInputTopic<String, TransactionEvent> input = inputTopic(driver);
            TestOutputTopic<String, LedgerBalance> balances = balancesTopic(driver);

            TransactionEvent credit = event("e1", "tenant-a", "acct-1", "100", TxDirection.CREDIT, "USD", 1_000L);
            input.pipeInput("ignored", credit);
            input.pipeInput("ignored", credit); // client retried the POST
            input.pipeInput("ignored", event("e2", "tenant-a", "acct-1", "5", TxDirection.CREDIT, "USD", 2_000L));

            List<LedgerBalance> outputs = balances.readValuesToList();
            assertThat(outputs).hasSize(2);
            assertThat(outputs.get(1).getBalance()).isEqualByComparingTo("105");
            assertThat(meterRegistry.counter("ledger.transactions.duplicates").count()).isEqualTo(1);
            assertThat(meterRegistry.counter("ledger.transactions.processed").count()).isEqualTo(2);
        }
    }

    @Test
    void scopesEventIdsPerAccount() {
        try (TopologyTestDriver driver = createDriver()) {
            TestInputTopic<String, TransactionEvent> input = inputTopic(driver);
            TestOutputTopic<String, LedgerBalance> balances = balancesTopic(driver);

            // Tenants choose their own event IDs, so the same ID in another tenant is not a duplicate
            input.pipeInput("ignored", event("e1", "tenant-a", "acct-1", "100", TxDirection.CREDIT, "USD", 1_000L));
            input.pipeInput("ignored", event("e1", "tenant-b", "acct-1", "100", TxDirection.CREDIT, "USD", 1_000L));

            assertThat(balances.readValuesToList()).hasSize(2);
        }
    }

    @Test
    void rejectsCurrencyMismatchToDlqWithoutTouchingBalance() {
        try (TopologyTestDriver driver = createDriver()) {
            TestInputTopic<String, TransactionEvent> input = inputTopic(driver);
            TestOutputTopic<String, LedgerBalance> balances = balancesTopic(driver);
            TestOutputTopic<String, TransactionEvent> dlq = driver.createOutputTopic(
                    Topics.LEDGER_DLQ, Serdes.String().deserializer(), transactionSerde().deserializer());

            input.pipeInput("ignored", event("e1", "tenant-a", "acct-1", "100", TxDirection.CREDIT, "USD", 1_000L));
            input.pipeInput("ignored", event("e2", "tenant-a", "acct-1", "100", TxDirection.CREDIT, "EUR", 2_000L));

            List<LedgerBalance> outputs = balances.readValuesToList();
            assertThat(outputs).hasSize(1);
            assertThat(outputs.get(0).getBalance()).isEqualByComparingTo("100");
            assertThat(outputs.get(0).getCurrency()).isEqualTo("USD");

            TestRecord<String, TransactionEvent> rejected = dlq.readRecord();
            assertThat(rejected.value().getEventId()).isEqualTo("e2");
            Header reason = rejected.headers().lastHeader(TransactionGuardProcessor.REJECT_REASON_HEADER);
            assertThat(new String(reason.value(), StandardCharsets.UTF_8)).contains("CURRENCY_MISMATCH");
            assertThat(meterRegistry.counter("dlq.records", "topic", Topics.LEDGER_DLQ).count()).isEqualTo(1);
        }
    }

    @Test
    void emitsFraudAlertWhenAmountThresholdIsExceeded() {
        try (TopologyTestDriver driver = createDriver()) {
            TestInputTopic<String, TransactionEvent> input = inputTopic(driver);
            TestOutputTopic<String, FraudAlert> fraudTopic = driver.createOutputTopic(
                    Topics.FRAUD_ALERTS,
                    Serdes.String().deserializer(),
                    fraudSerde().deserializer()
            );

            input.pipeInput("ignored", event("e3", "tenant-b", "acct-9", "25000", TxDirection.CREDIT, "USD", 1_500L));

            FraudAlert alert = fraudTopic.readValue();
            assertThat(alert.getTenantId()).isEqualTo("tenant-b");
            assertThat(alert.getAccountId()).isEqualTo("acct-9");
            assertThat(alert.getReason()).isEqualTo("SINGLE_TX_THRESHOLD_EXCEEDED");
            assertThat(meterRegistry.counter("fraud.alerts.emitted", "reason", "SINGLE_TX_THRESHOLD_EXCEEDED").count())
                    .isEqualTo(1);
        }
    }

    @Test
    void detectsBurstInHistoricalEventTimeRegardlessOfWallClock() {
        try (TopologyTestDriver driver = createDriver()) {
            TestInputTopic<String, TransactionEvent> input = inputTopic(driver);
            TestOutputTopic<String, FraudAlert> fraudTopic = fraudTopic(driver);

            // Event times from 2024 — far behind the wall clock. A wall-clock punctuator used to
            // evict this activity between events, so the burst was never detected.
            long base = 1_710_000_000_000L;
            for (int i = 0; i < 5; i++) {
                input.pipeInput("ignored", event("b" + i, "tenant-a", "acct-1", "10", TxDirection.CREDIT, "USD",
                        base + i * 1_000L));
                driver.advanceWallClockTime(Duration.ofMinutes(5));
            }

            List<FraudAlert> alerts = fraudTopic.readValuesToList();
            assertThat(alerts).extracting(FraudAlert::getReason).containsExactly("BURST_ACTIVITY_DETECTED");
        }
    }

    @Test
    void evictsIdleAccountsOnStreamTimeViaExpiryIndex() {
        try (TopologyTestDriver driver = createDriver()) {
            TestInputTopic<String, TransactionEvent> input = inputTopic(driver);
            KeyValueStore<String, AccountActivity> activity = driver.getKeyValueStore(Topics.FRAUD_ACTIVITY_STORE);
            KeyValueStore<String, String> index = driver.getKeyValueStore(Topics.FRAUD_EXPIRY_INDEX_STORE);

            input.pipeInput("ignored", event("e1", "tenant-a", "idle", "10", TxDirection.CREDIT, "USD", 1_000L));
            input.pipeInput("ignored", event("e2", "tenant-a", "busy", "10", TxDirection.CREDIT, "USD", 50_000L));
            assertThat(activity.get("tenant-a|idle")).isNotNull();

            // Stream time moves past idle's lastEventTime + 2 × burst window (120 s)
            input.pipeInput("ignored", event("e3", "tenant-a", "busy", "10", TxDirection.CREDIT, "USD", 200_000L));

            assertThat(activity.get("tenant-a|idle")).isNull();
            assertThat(activity.get("tenant-a|busy")).isNotNull();
            try (var entries = index.all()) {
                assertThat(entries).toIterable().extracting(kv -> kv.value).containsExactly("tenant-a|busy");
            }
        }
    }

    private TopologyTestDriver createDriver() {
        StreamsBuilder builder = new StreamsBuilder();
        LedgerTopologyFactory.build(builder, streamProperties(), new LedgerMetrics(meterRegistry), transactionSerde(), ledgerSerde(), fraudSerde());
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ledger-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY);
        // Same event-time semantics as application.yml
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionEventTimestampExtractor.class);

        return new TopologyTestDriver(topology, props);
    }

    private TestInputTopic<String, TransactionEvent> inputTopic(TopologyTestDriver driver) {
        return driver.createInputTopic(
                Topics.TRANSACTION_EVENTS,
                Serdes.String().serializer(),
                transactionSerde().serializer()
        );
    }

    private TestOutputTopic<String, LedgerBalance> balancesTopic(TopologyTestDriver driver) {
        return driver.createOutputTopic(
                Topics.LEDGER_BALANCES,
                Serdes.String().deserializer(),
                ledgerSerde().deserializer()
        );
    }

    private TestOutputTopic<String, FraudAlert> fraudTopic(TopologyTestDriver driver) {
        return driver.createOutputTopic(Topics.FRAUD_ALERTS, Serdes.String().deserializer(), fraudSerde().deserializer());
    }

    private LedgerStreamProperties streamProperties() {
        LedgerStreamProperties properties = new LedgerStreamProperties();
        properties.setSingleTransactionAlertThreshold(BigDecimal.valueOf(10_000));
        properties.setBurstWindowMs(60_000);
        properties.setBurstCountThreshold(5);
        properties.setDedupWindow(Duration.ofDays(1));
        return properties;
    }

    private Serde<TransactionEvent> transactionSerde() {
        SpecificAvroSerde<TransactionEvent> serde = new SpecificAvroSerde<>();
        serde.configure(schemaRegistryConfig(), false);
        return serde;
    }

    private Serde<LedgerBalance> ledgerSerde() {
        SpecificAvroSerde<LedgerBalance> serde = new SpecificAvroSerde<>();
        serde.configure(schemaRegistryConfig(), false);
        return serde;
    }

    private Serde<FraudAlert> fraudSerde() {
        SpecificAvroSerde<FraudAlert> serde = new SpecificAvroSerde<>();
        serde.configure(schemaRegistryConfig(), false);
        return serde;
    }

    private Map<String, String> schemaRegistryConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY);
        return config;
    }

    private TransactionEvent event(String eventId,
                                   String tenant,
                                   String account,
                                   String amount,
                                   TxDirection direction,
                                   String currency,
                                   long eventTime) {
        return TransactionEvent.newBuilder()
                .setEventId(eventId)
                .setTenantId(tenant)
                .setAccountId(account)
                .setAmount(new BigDecimal(amount).setScale(4))
                .setDirection(direction)
                .setCurrency(currency)
                .setEventTime(eventTime)
                .build();
    }
}
