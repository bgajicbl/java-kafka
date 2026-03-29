package com.example.ledger.config;

import com.example.ledger.avro.FraudAlert;
import com.example.ledger.avro.LedgerBalance;
import com.example.ledger.avro.PaymentEvent;
import com.example.ledger.avro.TenantRevenue;
import com.example.ledger.avro.TransactionEvent;
import com.example.ledger.model.PaymentState;
import com.example.ledger.model.SyncRunMergedRecord;
import com.example.ledger.streams.LedgerTopologyFactory;
import com.example.ledger.streams.PaymentTopologyFactory;
import com.example.ledger.streams.PaymentWindowedJoinTopologyFactory;
import com.example.ledger.streams.RevenueAggregationTopologyFactory;
import com.example.ledger.streams.SyncRunCdcTopologyFactory;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableKafkaStreams
@EnableConfigurationProperties(LedgerStreamProperties.class)
public class KafkaStreamsConfig {

    // ── Avro Serdes ──────────────────────────────────────────────────

    @Bean
    public Serde<TransactionEvent> transactionSerde(KafkaProperties kafkaProperties) {
        SpecificAvroSerde<TransactionEvent> serde = new SpecificAvroSerde<>();
        serde.configure(schemaRegistryConfig(kafkaProperties), false);
        return serde;
    }

    @Bean
    public Serde<LedgerBalance> ledgerBalanceSerde(KafkaProperties kafkaProperties) {
        SpecificAvroSerde<LedgerBalance> serde = new SpecificAvroSerde<>();
        serde.configure(schemaRegistryConfig(kafkaProperties), false);
        return serde;
    }

    @Bean
    public Serde<FraudAlert> fraudAlertSerde(KafkaProperties kafkaProperties) {
        SpecificAvroSerde<FraudAlert> serde = new SpecificAvroSerde<>();
        serde.configure(schemaRegistryConfig(kafkaProperties), false);
        return serde;
    }

    @Bean
    public Serde<PaymentEvent> paymentEventSerde(KafkaProperties kafkaProperties) {
        SpecificAvroSerde<PaymentEvent> serde = new SpecificAvroSerde<>();
        serde.configure(schemaRegistryConfig(kafkaProperties), false);
        return serde;
    }

    @Bean
    public Serde<TenantRevenue> tenantRevenueSerde(KafkaProperties kafkaProperties) {
        SpecificAvroSerde<TenantRevenue> serde = new SpecificAvroSerde<>();
        serde.configure(schemaRegistryConfig(kafkaProperties), false);
        return serde;
    }

    // ── Topologies ───────────────────────────────────────────────────

    @Bean
    public KStream<String, TransactionEvent> ledgerTopology(StreamsBuilder builder,
                                                            LedgerStreamProperties properties,
                                                            Serde<TransactionEvent> transactionSerde,
                                                            Serde<LedgerBalance> ledgerBalanceSerde,
                                                            Serde<FraudAlert> fraudAlertSerde) {
        return LedgerTopologyFactory.build(builder, properties, transactionSerde, ledgerBalanceSerde, fraudAlertSerde);
    }

    @Bean
    public KStream<String, SyncRunMergedRecord> syncRunCdcTopology(StreamsBuilder builder) {
        return SyncRunCdcTopologyFactory.build(builder);
    }

    @Bean
    public KStream<String, PaymentState> paymentTopology(StreamsBuilder builder,
                                                          Serde<PaymentEvent> paymentEventSerde) {
        return PaymentTopologyFactory.build(builder, paymentEventSerde);
    }

    @Bean
    public KStream<String, String> paymentWindowedJoinTopology(StreamsBuilder builder) {
        return PaymentWindowedJoinTopologyFactory.build(builder);
    }

    @Bean
    public String revenueTopology(StreamsBuilder builder,
                                  Serde<TenantRevenue> tenantRevenueSerde) {
        RevenueAggregationTopologyFactory.build(builder, tenantRevenueSerde);
        return "revenue-topology-registered";
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private Map<String, String> schemaRegistryConfig(KafkaProperties kafkaProperties) {
        Map<String, String> config = new HashMap<>();
        String schemaRegistryUrl = kafkaProperties.getProperties()
                .getOrDefault("schema.registry.url", "http://localhost:8081");
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return config;
    }
}
