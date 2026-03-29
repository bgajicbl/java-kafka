package com.example.ledger.streams;

import com.example.ledger.avro.LedgerBalance;
import com.example.ledger.avro.TenantRevenue;
import com.example.ledger.model.AccountActivity;
import com.example.ledger.model.BalanceView;
import com.example.ledger.model.FraudStateView;
import com.example.ledger.model.PaymentState;
import com.example.ledger.model.RevenueWindowEntry;
import com.example.ledger.model.SyncRunMergedRecord;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

/**
 * Interactive query service for all Kafka Streams state stores.
 *
 * <p>Each method reads from a materialised state store (backed by RocksDB)
 * to provide low-latency, strongly-consistent reads of the streaming state.
 */
@Service
public class LedgerQueryService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public LedgerQueryService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    // ── Ledger Balance ───────────────────────────────────────────────

    public Optional<BalanceView> findBalance(String tenantId, String accountId) {
        String key = tenantId + "|" + accountId;
        ReadOnlyKeyValueStore<String, LedgerBalance> store = getStore(Topics.BALANCE_STORE);
        LedgerBalance value = store.get(key);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(new BalanceView(
                value.getTenantId(),
                value.getAccountId(),
                value.getBalance(),
                value.getCurrency(),
                value.getLastEventTime()
        ));
    }

    // ── Fraud Activity ───────────────────────────────────────────────

    public Optional<FraudStateView> findFraudState(String tenantId, String accountId) {
        String key = tenantId + "|" + accountId;
        ReadOnlyKeyValueStore<String, AccountActivity> store = getStore(Topics.FRAUD_ACTIVITY_STORE);
        AccountActivity value = store.get(key);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(new FraudStateView(
                tenantId,
                accountId,
                value.getEventsInWindow(),
                value.getWindowStartMs(),
                value.getLastEventTime()
        ));
    }

    // ── CDC Sync Runs ────────────────────────────────────────────────

    public Optional<SyncRunMergedRecord> findSyncRun(String syncRunId) {
        ReadOnlyKeyValueStore<String, SyncRunMergedRecord> store = getStore(Topics.SYNC_RUNS_MERGED_STORE);
        SyncRunMergedRecord value = store.get(syncRunId);
        return Optional.ofNullable(value);
    }

    // ── Payment State ────────────────────────────────────────────────

    public Optional<PaymentState> findPaymentState(String tenantId, String paymentId) {
        String key = tenantId + "|" + paymentId;
        ReadOnlyKeyValueStore<String, PaymentState> store = getStore(Topics.PAYMENT_STATE_STORE);
        PaymentState value = store.get(key);
        return Optional.ofNullable(value);
    }

    // ── Revenue (Windowed) ───────────────────────────────────────────

    /**
     * Fetches hourly revenue windows for a tenant within a time range.
     * Queries the {@link ReadOnlyWindowStore} backed by the tumbling-window
     * aggregation in {@link RevenueAggregationTopologyFactory}.
     */
    public List<RevenueWindowEntry> findHourlyRevenue(String tenantId, Instant from, Instant to) {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null) {
            throw new IllegalStateException("Kafka Streams is not running yet");
        }

        ReadOnlyWindowStore<String, TenantRevenue> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        Topics.REVENUE_STORE,
                        QueryableStoreTypes.windowStore()
                )
        );

        List<RevenueWindowEntry> results = new ArrayList<>();
        try (WindowStoreIterator<TenantRevenue> iter = store.fetch(tenantId, from, to)) {
            while (iter.hasNext()) {
                KeyValue<Long, TenantRevenue> entry = iter.next();
                TenantRevenue rev = entry.value;
                results.add(new RevenueWindowEntry(
                        rev.getWindowStart(),
                        rev.getWindowEnd(),
                        rev.getTotalAmount(),
                        rev.getTransactionCount(),
                        rev.getCurrency()
                ));
            }
        }
        return results;
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private <T> ReadOnlyKeyValueStore<String, T> getStore(String storeName) {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null) {
            throw new IllegalStateException("Kafka Streams is not running yet");
        }
        return kafkaStreams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    }
}

