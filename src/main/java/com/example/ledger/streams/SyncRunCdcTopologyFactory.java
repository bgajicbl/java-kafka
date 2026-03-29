package com.example.ledger.streams;

import com.example.ledger.model.SyncRunMergedRecord;
import com.example.ledger.model.SyncRunPayloadAggregate;
import com.example.ledger.model.SyncRunPayloadChange;
import com.example.ledger.model.SyncRunPayloadRecord;
import com.example.ledger.model.SyncRunRecord;
import com.example.ledger.serde.JsonSerde;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public final class SyncRunCdcTopologyFactory {

    private SyncRunCdcTopologyFactory() {
    }

    public static KStream<String, SyncRunMergedRecord> build(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<SyncRunRecord> syncRunSerde = new JsonSerde<>(SyncRunRecord.class);
        JsonSerde<SyncRunPayloadChange> payloadChangeSerde = new JsonSerde<>(SyncRunPayloadChange.class);
        JsonSerde<SyncRunPayloadAggregate> payloadAggregateSerde = new JsonSerde<>(SyncRunPayloadAggregate.class);
        JsonSerde<SyncRunMergedRecord> mergedSerde = new JsonSerde<>(SyncRunMergedRecord.class);

        KTable<String, SyncRunRecord> syncRuns = builder
                .stream(Topics.SYNC_RUNS_CDC, Consumed.with(stringSerde, stringSerde))
                .flatMap((key, value) -> {
                    DebeziumCdcParser.DebeziumChange<SyncRunRecord> change = DebeziumCdcParser.parseSyncRunChange(value);
                    if (change == null || change.key() == null || change.key().isBlank()) {
                        return List.<KeyValue<String, SyncRunRecord>>of();
                    }
                    return List.of(KeyValue.pair(change.key(), change.deleted() ? null : change.value()));
                })
                .toTable(Materialized.<String, SyncRunRecord, KeyValueStore<Bytes, byte[]>>as(Topics.SYNC_RUNS_STORE)
                        .withKeySerde(stringSerde)
                        .withValueSerde(syncRunSerde));

        KTable<String, SyncRunPayloadAggregate> payloads = builder
                .stream(Topics.SYNC_RUN_PAYLOADS_CDC, Consumed.with(stringSerde, stringSerde))
                .flatMap((key, value) -> {
                    SyncRunPayloadChange change = DebeziumCdcParser.parseSyncRunPayloadChange(value);
                    if (change == null || change.syncRunId() == null || change.syncRunId().isBlank()) {
                        return List.<KeyValue<String, SyncRunPayloadChange>>of();
                    }
                    return List.of(KeyValue.pair(change.syncRunId(), change));
                })
                .groupByKey(Grouped.with(stringSerde, payloadChangeSerde))
                .aggregate(
                        SyncRunPayloadAggregate::new,
                        (syncRunId, change, aggregate) -> aggregate.apply(change),
                        Materialized.<String, SyncRunPayloadAggregate, KeyValueStore<Bytes, byte[]>>as(Topics.SYNC_RUN_PAYLOADS_STORE)
                                .withKeySerde(stringSerde)
                                .withValueSerde(payloadAggregateSerde)
                );

        KTable<String, SyncRunMergedRecord> merged = syncRuns.leftJoin(
                payloads,
                (syncRun, payloadAggregate) -> {
                    if (syncRun == null) {
                        return null;
                    }
                    List<SyncRunPayloadRecord> mergedPayloads = payloadAggregate == null
                            ? List.of()
                            : payloadAggregate.payloads();
                    return new SyncRunMergedRecord(syncRun.syncRunId(), syncRun, mergedPayloads, mergedPayloads.size());
                },
                Materialized.<String, SyncRunMergedRecord, KeyValueStore<Bytes, byte[]>>as(Topics.SYNC_RUNS_MERGED_STORE)
                        .withKeySerde(stringSerde)
                        .withValueSerde(mergedSerde)
        );

        KStream<String, SyncRunMergedRecord> mergedStream = merged.toStream();
        mergedStream.to(Topics.SYNC_RUNS_ENRICHED, Produced.with(stringSerde, mergedSerde));
        return mergedStream;
    }
}

