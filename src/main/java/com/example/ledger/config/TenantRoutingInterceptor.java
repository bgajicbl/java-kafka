package com.example.ledger.config;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka {@link ProducerInterceptor} that enriches outgoing records with
 * a {@code X-Tenant-Id} header extracted from composite keys.
 *
 * <p>Downstream consumers and monitoring systems can filter/route by tenant
 * without deserialising the message value — useful for multi-tenant SaaS
 * architectures with shared Kafka clusters.
 */
public class TenantRoutingInterceptor implements ProducerInterceptor<String, Object> {

    private static final Logger log = LoggerFactory.getLogger(TenantRoutingInterceptor.class);

    @Override
    public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> record) {
        String key = record.key();
        if (key != null && key.contains("|")) {
            String tenantId = key.substring(0, key.indexOf('|'));
            record.headers().add("X-Tenant-Id", tenantId.getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            log.warn("Failed to produce record to {}-{}: {}",
                    metadata != null ? metadata.topic() : "unknown",
                    metadata != null ? metadata.partition() : -1,
                    exception.getMessage());
        }
    }

    @Override
    public void close() {
        // Nothing to release
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed
    }
}

