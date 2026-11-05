package com.example.ledger.config;

import com.example.ledger.streams.DeadLetterQueueHandler;
import com.example.ledger.streams.Topics;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes records that cannot be deserialised to {@link Topics#DLQ_DESERIALIZATION} with their
 * original bytes and diagnostic headers, then skips them.
 *
 * <p>A poison pill is only skipped once it is <em>durably</em> in the DLQ: if the DLQ write
 * fails, the handler returns {@code FAIL} and the stream thread stops rather than lose the
 * record. The DLQ producer is separate from the Streams transactional producer, so the DLQ is
 * at-least-once — a crash between the DLQ write and the offset commit can dead-letter the same
 * record twice, which DLQ consumers must tolerate (dedupe on the original-coordinate headers).
 *
 * <p>Kafka instantiates this class reflectively, so it cannot receive Spring beans; the DLQ
 * counter goes to Micrometer's global registry, which Spring Boot bridges to Prometheus.
 *
 * <p>Register via
 * {@code spring.kafka.streams.properties.default.deserialization.exception.handler}.
 */
public class StreamsDeserializationErrorHandler implements DeserializationExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(StreamsDeserializationErrorHandler.class);
    private static final Duration DLQ_SEND_TIMEOUT = Duration.ofSeconds(10);

    private Producer<byte[], byte[]> producer;

    public StreamsDeserializationErrorHandler() {
    }

    StreamsDeserializationErrorHandler(Producer<byte[], byte[]> producer) {
        this.producer = producer;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        if (producer != null) {
            return;
        }
        // Reuse the app's connection/security settings; this producer is deliberately non-transactional
        Map<String, Object> producerConfig = new StreamsConfig(configs).getProducerConfigs("dlq-deserialization");
        producerConfig.remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producer = new KafkaProducer<>(producerConfig, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context,
                                                 ConsumerRecord<byte[], byte[]> record,
                                                 Exception exception) {
        ProducerRecord<byte[], byte[]> deadLetter = new ProducerRecord<>(
                Topics.DLQ_DESERIALIZATION, null, record.key(), record.value(),
                DeadLetterQueueHandler.enrichHeaders(record, exception));
        try {
            producer.send(deadLetter).get(DLQ_SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Interrupted dead-lettering {}-{}@{}; failing the stream thread",
                    record.topic(), record.partition(), record.offset(), ex);
            return DeserializationHandlerResponse.FAIL;
        } catch (Exception ex) {
            log.error("Could not dead-letter {}-{}@{}; failing the stream thread rather than dropping it",
                    record.topic(), record.partition(), record.offset(), ex);
            return DeserializationHandlerResponse.FAIL;
        }

        Metrics.counter("dlq.records", "topic", Topics.DLQ_DESERIALIZATION).increment();
        log.warn("Dead-lettered undeserialisable record {}-{}@{}: {}",
                record.topic(), record.partition(), record.offset(), exception.getMessage());
        return DeserializationHandlerResponse.CONTINUE;
    }
}
