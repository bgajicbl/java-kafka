package com.example.ledger.config;

import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * Custom {@link StreamPartitioner} that routes all data for a given tenant
 * to the same partition, regardless of the sub-key (accountId, paymentId, etc.).
 *
 * <p>Composite keys follow the convention {@code tenantId|subKey}. This partitioner
 * hashes only on {@code tenantId}, guaranteeing co-partitioned data for
 * multi-tenant joins and aggregations.
 *
 * <p>This is critical for SaaS architectures where tenant isolation and
 * co-partition requirements must be satisfied simultaneously.
 */
public class TenantAwarePartitioner<V> implements StreamPartitioner<String, V> {

    @Override
    public Integer partition(String topic, String key, V value, int numPartitions) {
        if (key == null) {
            return null; // let Kafka use default partitioning
        }
        String tenantId = key.contains("|") ? key.substring(0, key.indexOf('|')) : key;
        return Math.abs(tenantId.hashCode() % numPartitions);
    }
}

