package com.example.ledger.config;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams {@link ProductionExceptionHandler} for producer-side errors
 * (e.g. record-too-large, authorization failures).
 *
 * <p>Always {@code FAIL}s. The record that failed to produce is a balance update, a payment
 * state change or a fraud alert; returning {@code CONTINUE} would commit the input offset
 * and silently lose it, leaving the ledger inconsistent with its changelog. Failing stops
 * the stream thread; {@code KafkaStreamsConfig} then shuts the client down and the
 * {@code kafkaStreams} health indicator takes the pod out of service, so the incident is
 * loud and processing resumes from the last committed transaction once fixed.
 *
 * <p>Register via
 * {@code spring.kafka.streams.properties.default.production.exception.handler}.
 */
public class StreamsProductionExceptionHandler implements ProductionExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(StreamsProductionExceptionHandler.class);

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record,
                                                     Exception exception) {
        log.error("Production error – topic={}, partition={}, keySize={}, valueSize={}; failing the stream thread",
                record.topic(),
                record.partition(),
                record.key() != null ? record.key().length : 0,
                record.value() != null ? record.value().length : 0,
                exception);
        return ProductionExceptionHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed
    }
}
