package com.example.ledger.config;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams {@link DeserializationExceptionHandler} that logs poison pills
 * and continues processing instead of crashing the entire application.
 *
 * <p>In production, this would additionally produce the failed raw bytes to a
 * dead-letter topic (e.g., {@code dlq-deserialization-errors}) for later
 * investigation. Here we log and skip to keep the streams app resilient.
 *
 * <p>Register via
 * {@code spring.kafka.streams.properties.default.deserialization.exception.handler}.
 */
public class StreamsDeserializationErrorHandler implements DeserializationExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(StreamsDeserializationErrorHandler.class);

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context,
                                                  ConsumerRecord<byte[], byte[]> record,
                                                  Exception exception) {
        log.error("Deserialization error – topic={}, partition={}, offset={}, keySize={}, valueSize={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key() != null ? record.key().length : 0,
                record.value() != null ? record.value().length : 0,
                exception);

        // CONTINUE = skip this record and keep processing
        // FAIL     = shut down the streams task (use for critical pipelines)
        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed
    }
}

