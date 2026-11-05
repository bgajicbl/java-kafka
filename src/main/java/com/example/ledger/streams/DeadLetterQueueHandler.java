package com.example.ledger.streams;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Builds dead-letter-queue (DLQ) record headers.
 *
 * <p>Carries the original record's headers plus diagnostic metadata (source coordinates,
 * exception class, message and truncated stack trace) so that DLQ consumers can triage and
 * replay failures without access to the original processing context.
 */
public final class DeadLetterQueueHandler {

    static final int MAX_STACKTRACE_BYTES = 4096;

    private DeadLetterQueueHandler() {
    }

    public static Headers enrichHeaders(ConsumerRecord<?, ?> failed, Exception exception) {
        RecordHeaders headers = new RecordHeaders();
        for (Header header : failed.headers()) {
            headers.add(header);
        }
        add(headers, "dlq.original.topic", failed.topic());
        add(headers, "dlq.original.partition", String.valueOf(failed.partition()));
        add(headers, "dlq.original.offset", String.valueOf(failed.offset()));
        add(headers, "dlq.error.class", exception.getClass().getName());
        add(headers, "dlq.error.message", exception.getMessage() != null ? exception.getMessage() : "unknown");

        StringWriter sw = new StringWriter();
        exception.printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();
        if (stackTrace.length() > MAX_STACKTRACE_BYTES) {
            stackTrace = stackTrace.substring(0, MAX_STACKTRACE_BYTES);
        }
        add(headers, "dlq.error.stacktrace", stackTrace);
        return headers;
    }

    private static void add(Headers headers, String key, String value) {
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
    }
}
