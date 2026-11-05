package com.example.ledger.config;

import com.example.ledger.streams.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicConfig {

    @Bean
    public NewTopic transactionEventsTopic() {
        return TopicBuilder.name(Topics.TRANSACTION_EVENTS)
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic ledgerBalancesTopic() {
        return TopicBuilder.name(Topics.LEDGER_BALANCES)
                .partitions(6)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic fraudAlertsTopic() {
        return TopicBuilder.name(Topics.FRAUD_ALERTS)
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic ledgerDlqTopic() {
        return TopicBuilder.name(Topics.LEDGER_DLQ)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic syncRunsCdcTopic() {
        return TopicBuilder.name(Topics.SYNC_RUNS_CDC)
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic syncRunPayloadsCdcTopic() {
        return TopicBuilder.name(Topics.SYNC_RUN_PAYLOADS_CDC)
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic syncRunsEnrichedTopic() {
        return TopicBuilder.name(Topics.SYNC_RUNS_ENRICHED)
                .partitions(6)
                .replicas(1)
                .compact()
                .build();
    }

    // ── Payment Orchestration ────────────────────────────────────────

    @Bean
    public NewTopic paymentCommandsTopic() {
        return TopicBuilder.name(Topics.PAYMENT_COMMANDS)
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentEventsTopic() {
        return TopicBuilder.name(Topics.PAYMENT_EVENTS)
                .partitions(6)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic paymentDlqTopic() {
        return TopicBuilder.name(Topics.PAYMENT_DLQ)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentMatchedTopic() {
        return TopicBuilder.name(Topics.PAYMENT_MATCHED)
                .partitions(6)
                .replicas(1)
                .build();
    }

    // ── Revenue Analytics ────────────────────────────────────────────

    @Bean
    public NewTopic tenantRevenueHourlyTopic() {
        return TopicBuilder.name(Topics.TENANT_REVENUE_HOURLY)
                .partitions(6)
                .replicas(1)
                .build();
    }

    // ── Dead Letter Queues ───────────────────────────────────────────

    @Bean
    public NewTopic dlqDeserializationTopic() {
        return TopicBuilder.name(Topics.DLQ_DESERIALIZATION)
                .partitions(3)
                .replicas(1)
                .build();
    }
}

