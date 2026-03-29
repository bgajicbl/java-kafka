package com.example.ledger.config;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams {@link ProductionExceptionHandler} for producer-side errors
 * (e.g., serialisation failures, record-too-large).
 *
 * <p>Logs the error and continues processing. In production, integrate with
 * alerting systems (PagerDuty, Opsgenie) and consider routing to a DLQ.
 *
 * <p>Register via
 * {@code spring.kafka.streams.properties.default.production.exception.handler}.
 */
public class StreamsProductionExceptionHandler implements ProductionExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(StreamsProductionExceptionHandler.class);

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record,
                                                      Exception exception) {
        log.error("Production error – topic={}, partition={}, keySize={}: {}",
                record.topic(),
                record.partition(),
                record.key() != null ? record.key().length : 0,
                exception.getMessage(),
                exception);

        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed
    }
}

