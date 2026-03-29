package com.example.ledger.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.ledger.model.SyncRunMergedRecord;
import com.example.ledger.serde.JsonSerde;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

class SyncRunCdcTopologyTest {

    @Test
    void mergesSyncRunsAndPayloadsIntoOneTopic() {
        try (TopologyTestDriver driver = createDriver()) {
            TestInputTopic<String, String> syncRunsTopic = driver.createInputTopic(
                    Topics.SYNC_RUNS_CDC,
                    Serdes.String().serializer(),
                    Serdes.String().serializer()
            );
            TestInputTopic<String, String> payloadsTopic = driver.createInputTopic(
                    Topics.SYNC_RUN_PAYLOADS_CDC,
                    Serdes.String().serializer(),
                    Serdes.String().serializer()
            );
            TestOutputTopic<String, SyncRunMergedRecord> outputTopic = driver.createOutputTopic(
                    Topics.SYNC_RUNS_ENRICHED,
                    Serdes.String().deserializer(),
                    new JsonSerde<>(SyncRunMergedRecord.class).deserializer()
            );

            syncRunsTopic.pipeInput("ignored", """
                    {
                      "op":"c",
                      "after":{
                        "sync_run_id":"run-1",
                        "status":"STARTED",
                        "created_at":"2026-03-29T13:30:00Z",
                        "created_by":"tester",
                        "sync_job_id":"job-9",
                        "job_info":"initial run"
                      }
                    }
                    """);

            payloadsTopic.pipeInput("ignored", """
                    {
                      "op":"c",
                      "after":{
                        "sync_run_payload_id":"payload-1",
                        "sync_run_id":"run-1",
                        "transaction_id":"tx-77",
                        "order_number":"ord-10",
                        "order_amount":42.5,
                        "created_at":"2026-03-29T13:31:00Z",
                        "venue_id":"venue-1",
                        "customer_id":"cust-1",
                        "line_items":"[]",
                        "order_date":"2026-03-29T13:31:00Z",
                        "total_items":2,
                        "payments":"[]",
                        "note":"first payload",
                        "currency_code":"USD",
                        "currency_symbol":"$",
                        "visit_alias":"visit-1"
                      }
                    }
                    """);

            List<SyncRunMergedRecord> outputs = outputTopic.readValuesToList();
            assertThat(outputs).hasSize(2);

            SyncRunMergedRecord latest = outputs.get(1);
            assertThat(latest.syncRunId()).isEqualTo("run-1");
            assertThat(latest.syncRun().status()).isEqualTo("STARTED");
            assertThat(latest.payloadCount()).isEqualTo(1);
            assertThat(latest.payloads()).hasSize(1);
            assertThat(latest.payloads().get(0).transactionId()).isEqualTo("tx-77");
            assertThat(latest.payloads().get(0).orderAmount()).isEqualTo(42.5);
        }
    }

    private TopologyTestDriver createDriver() {
        StreamsBuilder builder = new StreamsBuilder();
        SyncRunCdcTopologyFactory.build(builder);
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sync-run-cdc-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        return new TopologyTestDriver(topology, props);
    }
}

