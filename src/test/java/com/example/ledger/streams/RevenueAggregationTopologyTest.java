package com.example.ledger.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.ledger.avro.TenantRevenue;
import com.example.ledger.model.PaymentState;
import com.example.ledger.serde.JsonSerde;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Instant;
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
 * Tests the tumbling-window hourly revenue aggregation topology.
 */
class RevenueAggregationTopologyTest {

    private static final String MOCK_SCHEMA_REGISTRY = "mock://revenue-test";

    @Test
    void aggregatesSettledPaymentsPerTenantHourly() {
        Serde<TenantRevenue> revenueSerde = revenueSerde();

        try (TopologyTestDriver driver = createDriver(revenueSerde)) {
            JsonSerde<PaymentState> stateSerde = new JsonSerde<>(PaymentState.class);

            TestInputTopic<String, PaymentState> input = driver.createInputTopic(
                    Topics.PAYMENT_EVENTS,
                    Serdes.String().serializer(),
                    stateSerde.serializer()
            );
            TestOutputTopic<String, TenantRevenue> output = driver.createOutputTopic(
                    Topics.TENANT_REVENUE_HOURLY,
                    Serdes.String().deserializer(),
                    revenueSerde.deserializer()
            );

            long hourBase = Instant.parse("2026-03-29T10:00:00Z").toEpochMilli();

            // Two SETTLED payments for tenant-a in the same hour
            input.pipeInput("tenant-a|pay-1", settledState("pay-1", "tenant-a", 100.0, hourBase + 60_000),
                    Instant.ofEpochMilli(hourBase + 60_000));
            input.pipeInput("tenant-a|pay-2", settledState("pay-2", "tenant-a", 250.0, hourBase + 120_000),
                    Instant.ofEpochMilli(hourBase + 120_000));

            // One INITIATED payment (should be filtered out)
            input.pipeInput("tenant-a|pay-3", new PaymentState(
                    "pay-3", "tenant-a", "acct-1", 999.0, "USD",
                    "INITIATED", null,
                    hourBase + 180_000, 0, 0, 0, 0, hourBase + 180_000
            ), Instant.ofEpochMilli(hourBase + 180_000));

            List<TenantRevenue> results = output.readValuesToList();
            // Two SETTLED → two incremental outputs
            assertThat(results).hasSize(2);

            TenantRevenue latest = results.get(1);
            assertThat(latest.getTenantId()).isEqualTo("tenant-a");
            assertThat(latest.getTotalAmount()).isEqualTo(350.0);
            assertThat(latest.getTransactionCount()).isEqualTo(2L);
            assertThat(latest.getWindowStart()).isGreaterThan(0);
        }
    }

    @Test
    void separatesTenants() {
        Serde<TenantRevenue> revenueSerde = revenueSerde();

        try (TopologyTestDriver driver = createDriver(revenueSerde)) {
            JsonSerde<PaymentState> stateSerde = new JsonSerde<>(PaymentState.class);

            TestInputTopic<String, PaymentState> input = driver.createInputTopic(
                    Topics.PAYMENT_EVENTS,
                    Serdes.String().serializer(),
                    stateSerde.serializer()
            );
            TestOutputTopic<String, TenantRevenue> output = driver.createOutputTopic(
                    Topics.TENANT_REVENUE_HOURLY,
                    Serdes.String().deserializer(),
                    revenueSerde.deserializer()
            );

            long hourBase = Instant.parse("2026-03-29T10:00:00Z").toEpochMilli();

            input.pipeInput("tenant-a|pay-1", settledState("pay-1", "tenant-a", 100.0, hourBase + 60_000),
                    Instant.ofEpochMilli(hourBase + 60_000));
            input.pipeInput("tenant-b|pay-2", settledState("pay-2", "tenant-b", 200.0, hourBase + 120_000),
                    Instant.ofEpochMilli(hourBase + 120_000));

            List<TenantRevenue> results = output.readValuesToList();
            assertThat(results).hasSize(2);

            // Each tenant gets its own aggregation
            assertThat(results.get(0).getTenantId()).isEqualTo("tenant-a");
            assertThat(results.get(0).getTotalAmount()).isEqualTo(100.0);
            assertThat(results.get(1).getTenantId()).isEqualTo("tenant-b");
            assertThat(results.get(1).getTotalAmount()).isEqualTo(200.0);
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private TopologyTestDriver createDriver(Serde<TenantRevenue> revenueSerde) {
        StreamsBuilder builder = new StreamsBuilder();
        RevenueAggregationTopologyFactory.build(builder, revenueSerde);
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "revenue-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY);

        return new TopologyTestDriver(topology, props);
    }

    private Serde<TenantRevenue> revenueSerde() {
        SpecificAvroSerde<TenantRevenue> serde = new SpecificAvroSerde<>();
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY);
        serde.configure(config, false);
        return serde;
    }

    private PaymentState settledState(String paymentId, String tenantId, double amount, long settledAt) {
        return new PaymentState(
                paymentId, tenantId, "acct-1", amount, "USD",
                "SETTLED", null,
                settledAt - 3000, settledAt - 2000, settledAt - 1000, settledAt, 0, settledAt
        );
    }
}

