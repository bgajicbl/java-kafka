package com.example.ledger.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.ledger.model.PaymentState;
import com.example.ledger.serde.JsonSerde;
import java.math.BigDecimal;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

/**
 * Tests the KStream–KStream windowed join that matches INITIATED payments
 * with their corresponding CAPTURED payments within a 10-minute window.
 */
class PaymentWindowedJoinTopologyTest {

    @Test
    void matchesInitiatedWithCapturedWithinWindow() {
        try (TopologyTestDriver driver = createDriver()) {
            JsonSerde<PaymentState> stateSerde = new JsonSerde<>(PaymentState.class);

            TestInputTopic<String, PaymentState> input = driver.createInputTopic(
                    Topics.PAYMENT_EVENTS,
                    Serdes.String().serializer(),
                    stateSerde.serializer()
            );
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    Topics.PAYMENT_MATCHED,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer()
            );

            String key = "tenant-a|pay-1";

            // Pipe INITIATED state at t=1000
            input.pipeInput(key, new PaymentState(
                    "pay-1", "tenant-a", "acct-1", new BigDecimal("100.0000"), "USD",
                    "INITIATED", null,
                    1000L, 0, 0, 0, 0, 1000L
            ), java.time.Instant.ofEpochMilli(1000));

            // Pipe CAPTURED state at t=5000 (within 10 min window)
            input.pipeInput(key, new PaymentState(
                    "pay-1", "tenant-a", "acct-1", new BigDecimal("100.0000"), "USD",
                    "CAPTURED", null,
                    1000L, 2000L, 5000L, 0, 0, 5000L
            ), java.time.Instant.ofEpochMilli(5000));

            List<String> matches = output.readValuesToList();
            assertThat(matches).hasSize(1);
            assertThat(matches.get(0)).contains("\"paymentId\":\"pay-1\"");
            assertThat(matches.get(0)).contains("\"amount\":100.0000");
            assertThat(matches.get(0)).contains("\"processingTimeMs\":4000");
        }
    }

    @Test
    void noMatchWhenDifferentKeys() {
        try (TopologyTestDriver driver = createDriver()) {
            JsonSerde<PaymentState> stateSerde = new JsonSerde<>(PaymentState.class);

            TestInputTopic<String, PaymentState> input = driver.createInputTopic(
                    Topics.PAYMENT_EVENTS,
                    Serdes.String().serializer(),
                    stateSerde.serializer()
            );
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    Topics.PAYMENT_MATCHED,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer()
            );

            // INITIATED for pay-1
            input.pipeInput("tenant-a|pay-1", new PaymentState(
                    "pay-1", "tenant-a", "acct-1", new BigDecimal("100.0000"), "USD",
                    "INITIATED", null,
                    1000L, 0, 0, 0, 0, 1000L
            ), java.time.Instant.ofEpochMilli(1000));

            // CAPTURED for pay-2 (different key — no match)
            input.pipeInput("tenant-a|pay-2", new PaymentState(
                    "pay-2", "tenant-a", "acct-1", new BigDecimal("200.0000"), "USD",
                    "CAPTURED", null,
                    1000L, 2000L, 5000L, 0, 0, 5000L
            ), java.time.Instant.ofEpochMilli(5000));

            assertThat(output.isEmpty()).isTrue();
        }
    }

    private TopologyTestDriver createDriver() {
        StreamsBuilder builder = new StreamsBuilder();
        PaymentWindowedJoinTopologyFactory.build(builder);
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-join-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        return new TopologyTestDriver(topology, props);
    }
}

