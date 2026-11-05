package com.example.ledger.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.ledger.avro.TenantRevenue;
import com.example.ledger.model.PaymentState;
import com.example.ledger.serde.JsonSerde;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import com.example.ledger.model.RevenueWindowEntry;
import java.math.BigDecimal;
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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
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
            input.pipeInput("tenant-a|pay-1", settledState("pay-1", "tenant-a", "100", hourBase + 60_000),
                    Instant.ofEpochMilli(hourBase + 60_000));
            input.pipeInput("tenant-a|pay-2", settledState("pay-2", "tenant-a", "250", hourBase + 120_000),
                    Instant.ofEpochMilli(hourBase + 120_000));

            // One INITIATED payment (should be filtered out)
            input.pipeInput("tenant-a|pay-3", new PaymentState(
                    "pay-3", "tenant-a", "acct-1", new BigDecimal("999.0000"), "USD",
                    "INITIATED", null,
                    hourBase + 180_000, 0, 0, 0, 0, hourBase + 180_000
            ), Instant.ofEpochMilli(hourBase + 180_000));

            List<TenantRevenue> results = output.readValuesToList();
            // Two SETTLED → two incremental outputs
            assertThat(results).hasSize(2);

            TenantRevenue latest = results.get(1);
            assertThat(latest.getTenantId()).isEqualTo("tenant-a");
            assertThat(latest.getTotalAmount()).isEqualByComparingTo("350");
            assertThat(latest.getTransactionCount()).isEqualTo(2L);
            assertThat(latest.getWindowStart()).isEqualTo(hourBase);
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

            input.pipeInput("tenant-a|pay-1", settledState("pay-1", "tenant-a", "100", hourBase + 60_000),
                    Instant.ofEpochMilli(hourBase + 60_000));
            input.pipeInput("tenant-b|pay-2", settledState("pay-2", "tenant-b", "200", hourBase + 120_000),
                    Instant.ofEpochMilli(hourBase + 120_000));

            List<TenantRevenue> results = output.readValuesToList();
            assertThat(results).hasSize(2);

            // Each tenant gets its own aggregation
            assertThat(results.get(0).getTenantId()).isEqualTo("tenant-a");
            assertThat(results.get(0).getTotalAmount()).isEqualByComparingTo("100");
            assertThat(results.get(1).getTenantId()).isEqualTo("tenant-b");
            assertThat(results.get(1).getTotalAmount()).isEqualByComparingTo("200");
        }
    }

    @Test
    void separatesCurrenciesWithinTenant() {
        Serde<TenantRevenue> revenueSerde = revenueSerde();

        try (TopologyTestDriver driver = createDriver(revenueSerde)) {
            TestInputTopic<String, PaymentState> input = paymentEventsTopic(driver);
            TestOutputTopic<String, TenantRevenue> output = driver.createOutputTopic(
                    Topics.TENANT_REVENUE_HOURLY,
                    Serdes.String().deserializer(),
                    revenueSerde.deserializer()
            );

            long hourBase = Instant.parse("2026-03-29T10:00:00Z").toEpochMilli();
            input.pipeInput("tenant-a|pay-1", settledState("pay-1", "tenant-a", "100", "USD", hourBase + 60_000),
                    Instant.ofEpochMilli(hourBase + 60_000));
            input.pipeInput("tenant-a|pay-2", settledState("pay-2", "tenant-a", "100", "EUR", hourBase + 120_000),
                    Instant.ofEpochMilli(hourBase + 120_000));

            List<KeyValue<String, TenantRevenue>> results = output.readKeyValuesToList();
            assertThat(results).extracting(kv -> kv.key).containsExactly("tenant-a|USD", "tenant-a|EUR");
            assertThat(results).allSatisfy(kv -> assertThat(kv.value.getTotalAmount()).isEqualByComparingTo("100"));
        }
    }

    @Test
    void queriedWindowsCarryRealBoundsFromStoreKey() {
        Serde<TenantRevenue> revenueSerde = revenueSerde();

        try (TopologyTestDriver driver = createDriver(revenueSerde)) {
            TestInputTopic<String, PaymentState> input = paymentEventsTopic(driver);
            long hourBase = Instant.parse("2026-03-29T10:00:00Z").toEpochMilli();
            input.pipeInput("tenant-a|pay-1", settledState("pay-1", "tenant-a", "100", hourBase + 60_000),
                    Instant.ofEpochMilli(hourBase + 60_000));

            WindowStore<String, TenantRevenue> store = driver.getWindowStore(Topics.REVENUE_STORE);
            try (WindowStoreIterator<TenantRevenue> iter = store.fetch(
                    RevenueAggregationTopologyFactory.revenueKey("tenant-a", "USD"),
                    Instant.ofEpochMilli(0), Instant.ofEpochMilli(hourBase + 3_600_000))) {
                KeyValue<Long, TenantRevenue> entry = iter.next();
                RevenueWindowEntry window = RevenueAggregationTopologyFactory.toWindowEntry(entry.key, entry.value);

                assertThat(window.windowStart()).isEqualTo(hourBase);
                assertThat(window.windowEnd()).isEqualTo(hourBase + 3_600_000);
                assertThat(window.totalAmount()).isEqualByComparingTo("100");
                assertThat(iter.hasNext()).isFalse();
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private TestInputTopic<String, PaymentState> paymentEventsTopic(TopologyTestDriver driver) {
        return driver.createInputTopic(
                Topics.PAYMENT_EVENTS,
                Serdes.String().serializer(),
                new JsonSerde<>(PaymentState.class).serializer()
        );
    }

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

    private PaymentState settledState(String paymentId, String tenantId, String amount, long settledAt) {
        return settledState(paymentId, tenantId, amount, "USD", settledAt);
    }

    private PaymentState settledState(String paymentId, String tenantId, String amount, String currency,
                                      long settledAt) {
        return new PaymentState(
                paymentId, tenantId, "acct-1", new BigDecimal(amount).setScale(4), currency,
                "SETTLED", null,
                settledAt - 3000, settledAt - 2000, settledAt - 1000, settledAt, 0, settledAt
        );
    }
}

