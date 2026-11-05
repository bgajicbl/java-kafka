package com.example.ledger.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.ledger.streams.Topics;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse;
import org.junit.jupiter.api.Test;

class StreamsDeserializationErrorHandlerTest {

    private final ConsumerRecord<byte[], byte[]> poisonPill = new ConsumerRecord<>(
            Topics.TRANSACTION_EVENTS, 2, 42L, "k".getBytes(StandardCharsets.UTF_8), new byte[]{0x0, 0x1});

    @Test
    void deadLettersOriginalBytesWithDiagnosticsThenContinues() {
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        var handler = new StreamsDeserializationErrorHandler(producer);

        DeserializationHandlerResponse response =
                handler.handle(null, poisonPill, new SerializationException("bad magic byte"));

        assertThat(response).isEqualTo(DeserializationHandlerResponse.CONTINUE);
        ProducerRecord<byte[], byte[]> deadLetter = producer.history().get(0);
        assertThat(deadLetter.topic()).isEqualTo(Topics.DLQ_DESERIALIZATION);
        assertThat(deadLetter.value()).isEqualTo(poisonPill.value());
        assertThat(header(deadLetter, "dlq.original.topic")).isEqualTo(Topics.TRANSACTION_EVENTS);
        assertThat(header(deadLetter, "dlq.original.partition")).isEqualTo("2");
        assertThat(header(deadLetter, "dlq.original.offset")).isEqualTo("42");
        assertThat(header(deadLetter, "dlq.error.message")).isEqualTo("bad magic byte");
    }

    @Test
    void failsTheThreadWhenTheDeadLetterCannotBeWritten() {
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
        producer.sendException = new RuntimeException("broker unavailable");
        var handler = new StreamsDeserializationErrorHandler(producer);

        DeserializationHandlerResponse response =
                handler.handle(null, poisonPill, new SerializationException("bad magic byte"));

        assertThat(response).isEqualTo(DeserializationHandlerResponse.FAIL);
    }

    private static String header(ProducerRecord<byte[], byte[]> record, String key) {
        return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
    }
}
