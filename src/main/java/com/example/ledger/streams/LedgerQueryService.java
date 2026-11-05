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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;

/**
 * Interactive query service for all Kafka Streams state stores.
 *
 * <p>Each lookup is routed through {@link InteractiveQueryRouter}: answered from the local
 * RocksDB store when this instance owns the key, otherwise forwarded to the owning instance.
 * The remote paths mirror the public REST endpoints in the {@code api} package.
 */
@Service
public class LedgerQueryService {

    private static final ParameterizedTypeReference<BalanceView> BALANCE_VIEW = new ParameterizedTypeReference<>() { };
    private static final ParameterizedTypeReference<FraudStateView> FRAUD_VIEW = new ParameterizedTypeReference<>() { };
    private static final ParameterizedTypeReference<SyncRunMergedRecord> SYNC_RUN = new ParameterizedTypeReference<>() { };
    private static final ParameterizedTypeReference<PaymentState> PAYMENT_STATE = new ParameterizedTypeReference<>() { };
    private static final ParameterizedTypeReference<List<RevenueWindowEntry>> REVENUE_WINDOWS =
            new ParameterizedTypeReference<>() { };

    private final InteractiveQueryRouter router;

    public LedgerQueryService(InteractiveQueryRouter router) {
        this.router = router;
    }

    // ── Ledger Balance ───────────────────────────────────────────────

    public Optional<BalanceView> findBalance(String tenantId, String accountId) {
        String key = tenantId + "|" + accountId;
        return router.query(Topics.BALANCE_STORE, key,
                uri -> uri.path("/api/ledger/{tenantId}/{accountId}").build(tenantId, accountId),
                BALANCE_VIEW,
                () -> {
                    LedgerBalance value = this.<LedgerBalance>keyValueStore(Topics.BALANCE_STORE).get(key);
                    return Optional.ofNullable(value).map(balance -> new BalanceView(
                            balance.getTenantId(),
                            balance.getAccountId(),
                            balance.getBalance(),
                            balance.getCurrency(),
                            balance.getLastEventTime()));
                });
    }

    // ── Fraud Activity ───────────────────────────────────────────────

    public Optional<FraudStateView> findFraudState(String tenantId, String accountId) {
        String key = tenantId + "|" + accountId;
        return router.query(Topics.FRAUD_ACTIVITY_STORE, key,
                uri -> uri.path("/api/fraud/{tenantId}/{accountId}").build(tenantId, accountId),
                FRAUD_VIEW,
                () -> {
                    AccountActivity value = this.<AccountActivity>keyValueStore(Topics.FRAUD_ACTIVITY_STORE).get(key);
                    return Optional.ofNullable(value).map(activity -> new FraudStateView(
                            tenantId,
                            accountId,
                            activity.getEventsInWindow(),
                            activity.getWindowStartMs(),
                            activity.getLastEventTime()));
                });
    }

    // ── CDC Sync Runs ────────────────────────────────────────────────

    public Optional<SyncRunMergedRecord> findSyncRun(String syncRunId) {
        return router.query(Topics.SYNC_RUNS_MERGED_STORE, syncRunId,
                uri -> uri.path("/api/sync-runs/{syncRunId}").build(syncRunId),
                SYNC_RUN,
                () -> Optional.ofNullable(
                        this.<SyncRunMergedRecord>keyValueStore(Topics.SYNC_RUNS_MERGED_STORE).get(syncRunId)));
    }

    // ── Payment State ────────────────────────────────────────────────

    public Optional<PaymentState> findPaymentState(String tenantId, String paymentId) {
        String key = tenantId + "|" + paymentId;
        return router.query(Topics.PAYMENT_STATE_STORE, key,
                uri -> uri.path("/api/payments/{tenantId}/{paymentId}").build(tenantId, paymentId),
                PAYMENT_STATE,
                () -> Optional.ofNullable(this.<PaymentState>keyValueStore(Topics.PAYMENT_STATE_STORE).get(key)));
    }

    // ── Revenue (Windowed) ───────────────────────────────────────────

    /**
     * Fetches hourly revenue windows for one tenant and currency within a time range.
     * Window bounds come from the window-store key, not the stored aggregate.
     */
    public List<RevenueWindowEntry> findHourlyRevenue(String tenantId, String currency, Instant from, Instant to) {
        String key = RevenueAggregationTopologyFactory.revenueKey(tenantId, currency);
        return router.query(Topics.REVENUE_STORE, key,
                uri -> uri.path("/api/revenue/{tenantId}")
                        .queryParam("currency", currency)
                        .queryParam("from", from.toEpochMilli())
                        .queryParam("to", to.toEpochMilli())
                        .build(tenantId),
                REVENUE_WINDOWS,
                () -> Optional.of(localHourlyRevenue(key, from, to)))
                .orElse(List.of());
    }

    private List<RevenueWindowEntry> localHourlyRevenue(String key, Instant from, Instant to) {
        ReadOnlyWindowStore<String, TenantRevenue> store =
                router.localStore(Topics.REVENUE_STORE, QueryableStoreTypes.windowStore());
        List<RevenueWindowEntry> results = new ArrayList<>();
        try (WindowStoreIterator<TenantRevenue> iter = store.fetch(key, from, to)) {
            while (iter.hasNext()) {
                KeyValue<Long, TenantRevenue> entry = iter.next();
                results.add(RevenueAggregationTopologyFactory.toWindowEntry(entry.key, entry.value));
            }
        }
        return results;
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private <T> ReadOnlyKeyValueStore<String, T> keyValueStore(String storeName) {
        return router.localStore(storeName, QueryableStoreTypes.keyValueStore());
    }
}
