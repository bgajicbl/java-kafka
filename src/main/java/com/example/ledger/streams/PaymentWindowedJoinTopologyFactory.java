package com.example.ledger.streams;

import com.example.ledger.model.PaymentState;
import com.example.ledger.serde.JsonSerde;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;

/**
 * Windowed KStream–KStream join topology that matches payment initiations
 * with their corresponding captures within a 10-minute window.
 *
 * <h3>Purpose</h3>
 * Demonstrates one of the most important Kafka Streams patterns for
 * financial platforms: correlating two events that must occur within a
 * bounded time frame (e.g., authorise ↔ capture, request ↔ confirmation).
 *
 * <h3>Topology</h3>
 * <pre>
 *   payment-events
 *       │
 *       ├── filter(INITIATED) ──┐
 *       │                       ├── KStream-KStream join (10 min window)
 *       └── filter(CAPTURED) ───┘           │
 *                                           ▼
 *                                    payment-matched
 * </pre>
 */
public final class PaymentWindowedJoinTopologyFactory {

    private PaymentWindowedJoinTopologyFactory() {
    }

    public static KStream<String, String> build(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<PaymentState> stateSerde = new JsonSerde<>(PaymentState.class);

        KStream<String, PaymentState> paymentEvents = builder
                .stream(Topics.PAYMENT_EVENTS, Consumed.with(stringSerde, stateSerde));

        // ── Split by lifecycle stage ─────────────────────────────────
        KStream<String, PaymentState> initiated = paymentEvents
                .filter((key, state) -> "INITIATED".equals(state.status()),
                        Named.as("filter-initiated"));

        KStream<String, PaymentState> captured = paymentEvents
                .filter((key, state) -> "CAPTURED".equals(state.status()),
                        Named.as("filter-captured"));

        // ── Windowed join: match initiated → captured within 10 min ──
        KStream<String, String> matched = initiated.join(
                captured,
                (init, capt) -> String.format(
                        "{\"paymentId\":\"%s\",\"tenantId\":\"%s\",\"amount\":%.2f,"
                                + "\"initiatedAt\":%d,\"capturedAt\":%d,\"processingTimeMs\":%d}",
                        init.paymentId(), init.tenantId(), init.amount(),
                        init.initiatedAt(), capt.capturedAt(),
                        capt.capturedAt() - init.initiatedAt()
                ),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(10)),
                StreamJoined.with(stringSerde, stateSerde, stateSerde)
        );

        matched.to(Topics.PAYMENT_MATCHED, Produced.with(stringSerde, stringSerde));

        return matched;
    }
}

