package com.example.ledger.streams;

import com.example.ledger.avro.TenantRevenue;
import com.example.ledger.model.PaymentState;
import com.example.ledger.serde.JsonSerde;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Tumbling-window aggregation that computes hourly revenue per tenant
 * from settled payments.
 *
 * <h3>Topology</h3>
 * <pre>
 *   payment-events
 *       │
 *       ▼  filter(status == SETTLED)
 *       │
 *       ▼  selectKey → tenantId
 *       │
 *       ▼  windowedBy(1-hour tumbling)
 *       │
 *       ▼  aggregate(sum amounts, count transactions)
 *       │         materialised as → revenue-hourly-store (WindowStore)
 *       │
 *       ▼  toStream → map(embed window boundaries)
 *       │
 *       ▼  tenant-revenue-hourly
 * </pre>
 *
 * <p>Demonstrates: tumbling time windows, windowed aggregation,
 * {@link WindowStore} materialisation, windowed interactive queries.
 */
public final class RevenueAggregationTopologyFactory {

    private RevenueAggregationTopologyFactory() {
    }

    public static void build(StreamsBuilder builder,
                             Serde<TenantRevenue> revenueSerde) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<PaymentState> stateSerde = new JsonSerde<>(PaymentState.class);

        // ── Source: only SETTLED payments ─────────────────────────────
        KStream<String, PaymentState> settled = builder
                .stream(Topics.PAYMENT_EVENTS, Consumed.with(stringSerde, stateSerde))
                .filter((key, state) -> state != null && "SETTLED".equals(state.status()),
                        Named.as("revenue-filter-settled"))
                .selectKey((key, state) -> state.tenantId(),
                        Named.as("revenue-rekey-tenant"));

        // ── 1-hour tumbling window aggregation ───────────────────────
        KTable<Windowed<String>, TenantRevenue> hourlyRevenue = settled
                .groupByKey(Grouped.with(stringSerde, stateSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
                .aggregate(
                        () -> TenantRevenue.newBuilder()
                                .setTenantId("unknown")
                                .setWindowStart(0L)
                                .setWindowEnd(0L)
                                .setTotalAmount(0.0)
                                .setTransactionCount(0L)
                                .setCurrency("USD")
                                .build(),
                        (tenantId, payment, aggregate) -> TenantRevenue.newBuilder()
                                .setTenantId(tenantId)
                                .setWindowStart(0L) // set from windowed key below
                                .setWindowEnd(0L)
                                .setTotalAmount(aggregate.getTotalAmount() + payment.amount())
                                .setTransactionCount(aggregate.getTransactionCount() + 1)
                                .setCurrency(payment.currency() != null ? payment.currency() : "USD")
                                .build(),
                        Materialized.<String, TenantRevenue, WindowStore<Bytes, byte[]>>as(Topics.REVENUE_STORE)
                                .withKeySerde(stringSerde)
                                .withValueSerde(revenueSerde)
                );

        // ── Output: embed window start/end in the record ─────────────
        hourlyRevenue.toStream()
                .map((windowedKey, revenue) -> KeyValue.pair(
                        windowedKey.key(),
                        TenantRevenue.newBuilder(revenue)
                                .setWindowStart(windowedKey.window().start())
                                .setWindowEnd(windowedKey.window().end())
                                .build()
                ))
                .to(Topics.TENANT_REVENUE_HOURLY, Produced.with(stringSerde, revenueSerde));
    }
}

