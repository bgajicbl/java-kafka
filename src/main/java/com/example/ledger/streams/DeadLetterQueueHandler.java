package com.example.ledger.streams;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reusable utility for building dead-letter-queue (DLQ) record headers.
 *
 * <p>Attaches diagnostic metadata (original topic, partition, exception stack trace)
 * to headers so that DLQ consumers can triage failures without access to the
 * original processing context.
 *
 * <p>Usage: call {@link #enrichHeaders} when routing a failed record to a DLQ topic.
 */
public final class DeadLetterQueueHandler {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueHandler.class);

    private DeadLetterQueueHandler() {
    }

    /**
     * Build headers for a DLQ record containing diagnostic information.
     *
     * @param originalTopic the topic the record was consumed from
     * @param exception     the exception that caused the failure
     * @return headers to attach to the DLQ ProducerRecord
     */
    public static Headers enrichHeaders(String originalTopic, Exception exception) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("dlq.original.topic", originalTopic.getBytes(StandardCharsets.UTF_8));
        headers.add("dlq.error.message",
                exception.getMessage() != null
                        ? exception.getMessage().getBytes(StandardCharsets.UTF_8)
                        : "unknown".getBytes(StandardCharsets.UTF_8));

        StringWriter sw = new StringWriter();
        exception.printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();
        if (stackTrace.length() > 4096) {
            stackTrace = stackTrace.substring(0, 4096);
        }
        headers.add("dlq.error.stacktrace", stackTrace.getBytes(StandardCharsets.UTF_8));

        log.warn("DLQ enrichment: topic={}, error={}", originalTopic, exception.getMessage());
        return headers;
    }
}

