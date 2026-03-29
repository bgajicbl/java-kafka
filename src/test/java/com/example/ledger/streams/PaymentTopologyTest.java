package com.example.ledger.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.ledger.avro.PaymentCommandType;
import com.example.ledger.avro.PaymentEvent;
import com.example.ledger.model.PaymentState;
import com.example.ledger.serde.JsonSerde;
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

/**
 * Tests the payment orchestration state-machine topology:
 * valid lifecycle, reversal path, and invalid-transition → DLQ routing.
 */
class PaymentTopologyTest {

    private static final String MOCK_SCHEMA_REGISTRY = "mock://payment-topology-test";

    @Test
    void fullLifecycle_initiateToSettle() {
        try (TopologyTestDriver driver = createDriver()) {
            TestInputTopic<String, PaymentEvent> input = inputTopic(driver);
            TestOutputTopic<String, PaymentState> output = outputTopic(driver, Topics.PAYMENT_EVENTS);
            TestOutputTopic<String, PaymentState> dlq = outputTopic(driver, Topics.PAYMENT_DLQ);

            input.pipeInput("key", command("pay-1", "tenant-a", "acct-1", 100.0, PaymentCommandType.INITIATE, 1000L));
            input.pipeInput("key", command("pay-1", "tenant-a", "acct-1", 100.0, PaymentCommandType.AUTHORIZE, 2000L));
            input.pipeInput("key", command("pay-1", "tenant-a", "acct-1", 100.0, PaymentCommandType.CAPTURE, 3000L));
            input.pipeInput("key", command("pay-1", "tenant-a", "acct-1", 100.0, PaymentCommandType.SETTLE, 4000L));

            List<PaymentState> states = output.readValuesToList();
            assertThat(states).hasSize(4);
            assertThat(states.get(0).status()).isEqualTo("INITIATED");
            assertThat(states.get(1).status()).isEqualTo("AUTHORIZED");
            assertThat(states.get(2).status()).isEqualTo("CAPTURED");
            assertThat(states.get(3).status()).isEqualTo("SETTLED");
            assertThat(states.get(3).settledAt()).isEqualTo(4000L);

            assertThat(dlq.isEmpty()).isTrue();
        }
    }

    @Test
    void reversalPath_initiateAuthorizeReverse() {
        try (TopologyTestDriver driver = createDriver()) {
            TestInputTopic<String, PaymentEvent> input = inputTopic(driver);
            TestOutputTopic<String, PaymentState> output = outputTopic(driver, Topics.PAYMENT_EVENTS);

            input.pipeInput("key", command("pay-2", "tenant-a", "acct-1", 50.0, PaymentCommandType.INITIATE, 1000L));
            input.pipeInput("key", command("pay-2", "tenant-a", "acct-1", 50.0, PaymentCommandType.AUTHORIZE, 2000L));
            input.pipeInput("key", command("pay-2", "tenant-a", "acct-1", 50.0, PaymentCommandType.REVERSE, 3000L));

            List<PaymentState> states = output.readValuesToList();
            assertThat(states).hasSize(3);
            assertThat(states.get(2).status()).isEqualTo("REVERSED");
            assertThat(states.get(2).reversedAt()).isEqualTo(3000L);
        }
    }

    @Test
    void invalidTransition_routesToDlq() {
        try (TopologyTestDriver driver = createDriver()) {
            TestInputTopic<String, PaymentEvent> input = inputTopic(driver);
            TestOutputTopic<String, PaymentState> output = outputTopic(driver, Topics.PAYMENT_EVENTS);
            TestOutputTopic<String, PaymentState> dlq = outputTopic(driver, Topics.PAYMENT_DLQ);

            input.pipeInput("key", command("pay-3", "tenant-b", "acct-2", 75.0, PaymentCommandType.INITIATE, 1000L));
            // Invalid: try to SETTLE directly from INITIATED (skipping AUTHORIZE + CAPTURE)
            input.pipeInput("key", command("pay-3", "tenant-b", "acct-2", 75.0, PaymentCommandType.SETTLE, 2000L));

            List<PaymentState> successes = output.readValuesToList();
            assertThat(successes).hasSize(1);
            assertThat(successes.get(0).status()).isEqualTo("INITIATED");

            List<PaymentState> failures = dlq.readValuesToList();
            assertThat(failures).hasSize(1);
            assertThat(failures.get(0).failReason()).contains("Cannot SETTLE");
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private TopologyTestDriver createDriver() {
        StreamsBuilder builder = new StreamsBuilder();
        PaymentTopologyFactory.build(builder, paymentEventSerde());
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "payment-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY);

        return new TopologyTestDriver(topology, props);
    }

    private TestInputTopic<String, PaymentEvent> inputTopic(TopologyTestDriver driver) {
        return driver.createInputTopic(
                Topics.PAYMENT_COMMANDS,
                Serdes.String().serializer(),
                paymentEventSerde().serializer()
        );
    }

    private TestOutputTopic<String, PaymentState> outputTopic(TopologyTestDriver driver, String topicName) {
        return driver.createOutputTopic(
                topicName,
                Serdes.String().deserializer(),
                new JsonSerde<>(PaymentState.class).deserializer()
        );
    }

    private Serde<PaymentEvent> paymentEventSerde() {
        SpecificAvroSerde<PaymentEvent> serde = new SpecificAvroSerde<>();
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY);
        serde.configure(config, false);
        return serde;
    }

    private PaymentEvent command(String paymentId, String tenantId, String accountId,
                                  double amount, PaymentCommandType type, long eventTime) {
        return PaymentEvent.newBuilder()
                .setPaymentId(paymentId)
                .setTenantId(tenantId)
                .setAccountId(accountId)
                .setAmount(amount)
                .setCurrency("USD")
                .setCommandType(type)
                .setCorrelationId(null)
                .setEventTime(eventTime)
                .build();
    }
}

