package com.example.ledger.streams;

import com.example.ledger.avro.PaymentEvent;
import com.example.ledger.model.PaymentState;
import com.example.ledger.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Payment orchestration topology — models the full payment lifecycle as a
 * stateful Kafka Streams aggregate backed by a RocksDB state store.
 *
 * <h3>Topology</h3>
 * <pre>
 *   payment-commands (Avro PaymentEvent)
 *       │
 *       ▼  selectKey → tenantId|paymentId
 *       │
 *       ▼  groupByKey → aggregate(PaymentStateMachine::transition)
 *       │         materialized as → payment-state-store
 *       │
 *       ├── filter(failReason == null)  → payment-events  (successful transitions)
 *       └── filter(failReason != null)  → payment-dlq     (invalid transitions)
 * </pre>
 *
 * <p>Demonstrates: stateful aggregation, KTable materialisation, event-sourced
 * state machine, dead-letter queue branching, and interactive queries.
 */
public final class PaymentTopologyFactory {

    private PaymentTopologyFactory() {
    }

    public static KStream<String, PaymentState> build(StreamsBuilder builder,
                                                       Serde<PaymentEvent> paymentEventSerde) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<PaymentState> paymentStateSerde = new JsonSerde<>(PaymentState.class);

        // ── Source: payment commands ──────────────────────────────────
        KStream<String, PaymentEvent> commands = builder
                .stream(Topics.PAYMENT_COMMANDS, Consumed.with(stringSerde, paymentEventSerde))
                .selectKey((key, value) -> value.getTenantId() + "|" + value.getPaymentId(),
                        Named.as("payment-rekey"));

        // ── Aggregate into payment state (state machine) ─────────────
        KTable<String, PaymentState> paymentStates = commands
                .groupByKey(Grouped.with(stringSerde, paymentEventSerde))
                .aggregate(
                        PaymentState::initial,
                        (key, command, currentState) -> PaymentStateMachine.transition(currentState, command),
                        Materialized.<String, PaymentState, KeyValueStore<Bytes, byte[]>>as(Topics.PAYMENT_STATE_STORE)
                                .withKeySerde(stringSerde)
                                .withValueSerde(paymentStateSerde)
                );

        KStream<String, PaymentState> stateChanges = paymentStates.toStream();

        // ── Successful transitions → payment-events ──────────────────
        stateChanges
                .filter((key, state) -> state != null && state.failReason() == null,
                        Named.as("payment-success-filter"))
                .to(Topics.PAYMENT_EVENTS, Produced.with(stringSerde, paymentStateSerde));

        // ── Failed transitions → payment-dlq ─────────────────────────
        stateChanges
                .filter((key, state) -> state != null && state.failReason() != null,
                        Named.as("payment-dlq-filter"))
                .to(Topics.PAYMENT_DLQ, Produced.with(stringSerde, paymentStateSerde));

        return stateChanges;
    }
}

