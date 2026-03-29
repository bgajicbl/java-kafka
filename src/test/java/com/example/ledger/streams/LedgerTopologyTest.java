package com.example.ledger.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.ledger.avro.FraudAlert;
import com.example.ledger.avro.LedgerBalance;
import com.example.ledger.avro.TransactionEvent;
import com.example.ledger.avro.TxDirection;
import com.example.ledger.config.LedgerStreamProperties;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

class LedgerTopologyTest {

    private static final String MOCK_SCHEMA_REGISTRY = "mock://ledger-topology-test";

    @Test
    void aggregatesBalancesPerTenantAndAccount() {
        try (TopologyTestDriver driver = createDriver()) {
            TestInputTopic<String, TransactionEvent> inputTopic = driver.createInputTopic(
                    Topics.TRANSACTION_EVENTS,
                    Serdes.String().serializer(),
                    transactionSerde().serializer()
            );
            TestOutputTopic<String, LedgerBalance> balancesTopic = driver.createOutputTopic(
                    Topics.LEDGER_BALANCES,
                    Serdes.String().deserializer(),
                    ledgerSerde().deserializer()
            );

            inputTopic.pipeInput("ignored", event("e1", "tenant-a", "acct-1", 1000.0, TxDirection.CREDIT, 1_000L));
            inputTopic.pipeInput("ignored", event("e2", "tenant-a", "acct-1", 250.0, TxDirection.DEBIT, 2_000L));

            List<LedgerBalance> outputs = balancesTopic.readValuesToList();
            assertThat(outputs).hasSize(2);
            LedgerBalance latest = outputs.get(1);

            assertThat(latest.getTenantId()).isEqualTo("tenant-a");
            assertThat(latest.getAccountId()).isEqualTo("acct-1");
            assertThat(latest.getBalance()).isEqualTo(750.0);
        }
    }

    @Test
    void emitsFraudAlertWhenAmountThresholdIsExceeded() {
        try (TopologyTestDriver driver = createDriver()) {
            TestInputTopic<String, TransactionEvent> inputTopic = driver.createInputTopic(
                    Topics.TRANSACTION_EVENTS,
                    Serdes.String().serializer(),
                    transactionSerde().serializer()
            );
            TestOutputTopic<String, FraudAlert> fraudTopic = driver.createOutputTopic(
                    Topics.FRAUD_ALERTS,
                    Serdes.String().deserializer(),
                    fraudSerde().deserializer()
            );

            inputTopic.pipeInput("ignored", event("e3", "tenant-b", "acct-9", 25_000.0, TxDirection.CREDIT, 1_500L));

            FraudAlert alert = fraudTopic.readValue();
            assertThat(alert.getTenantId()).isEqualTo("tenant-b");
            assertThat(alert.getAccountId()).isEqualTo("acct-9");
            assertThat(alert.getReason()).isEqualTo("SINGLE_TX_THRESHOLD_EXCEEDED");
        }
    }

    private TopologyTestDriver createDriver() {
        StreamsBuilder builder = new StreamsBuilder();
        LedgerTopologyFactory.build(builder, streamProperties(), transactionSerde(), ledgerSerde(), fraudSerde());
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ledger-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY);

        return new TopologyTestDriver(topology, props);
    }

    private LedgerStreamProperties streamProperties() {
        LedgerStreamProperties properties = new LedgerStreamProperties();
        properties.setSingleTransactionAlertThreshold(10_000);
        properties.setBurstWindowMs(60_000);
        properties.setBurstCountThreshold(5);
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
                                   double amount,
                                   TxDirection direction,
                                   long eventTime) {
        return TransactionEvent.newBuilder()
                .setEventId(eventId)
                .setTenantId(tenant)
                .setAccountId(account)
                .setAmount(amount)
                .setDirection(direction)
                .setCurrency("USD")
                .setEventTime(eventTime)
                .build();
    }
}

