package com.example.ledger.config;

import java.util.EnumSet;
import java.util.Set;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/**
 * Reports the Kafka Streams client state as the {@code kafkaStreams} health component.
 *
 * <p>Included in the liveness and readiness groups (see {@code application.yml}): a client in
 * {@code ERROR} or shut down never recovers by itself, so the pod must be restarted rather
 * than keep answering HTTP while no longer processing.
 */
@Component("kafkaStreams")
public class KafkaStreamsHealthIndicator implements HealthIndicator {

    private static final Set<State> DEAD = EnumSet.of(
            State.PENDING_ERROR, State.ERROR, State.PENDING_SHUTDOWN, State.NOT_RUNNING);

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public KafkaStreamsHealthIndicator(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    @Override
    public Health health() {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null) {
            return Health.unknown().withDetail("state", "NOT_CREATED").build();
        }
        State state = kafkaStreams.state();
        return (DEAD.contains(state) ? Health.down() : Health.up())
                .withDetail("state", state.name())
                .build();
    }
}
