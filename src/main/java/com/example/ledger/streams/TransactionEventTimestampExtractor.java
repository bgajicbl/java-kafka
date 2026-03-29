package com.example.ledger.streams;

import com.example.ledger.avro.TransactionEvent;
import com.example.ledger.avro.PaymentEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Custom {@link TimestampExtractor} that uses the domain event time embedded
 * in Avro records rather than the Kafka broker timestamp.
 *
 * <p>This ensures event-time semantics for windowed joins, windowed aggregations,
 * and any time-dependent logic in the topology.  Falls back to the partition-time
 * (previous record timestamp) for non-Avro or unrecognised record types.
 */
public class TransactionEventTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Object value = record.value();

        if (value instanceof TransactionEvent txEvent) {
            long ts = txEvent.getEventTime();
            return ts > 0 ? ts : partitionTime;
        }
        if (value instanceof PaymentEvent payEvent) {
            long ts = payEvent.getEventTime();
            return ts > 0 ? ts : partitionTime;
        }

        // Fallback: use the timestamp from the previous successfully extracted record
        return partitionTime;
    }
}

