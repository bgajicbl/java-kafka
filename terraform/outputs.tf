output "topic_names" {
  description = "All provisioned Kafka topic names"
  value = [
    kafka_topic.tx_events.name,
    kafka_topic.ledger_balances.name,
    kafka_topic.fraud_alerts.name,
    kafka_topic.payment_commands.name,
    kafka_topic.payment_events.name,
    kafka_topic.payment_dlq.name,
    kafka_topic.payment_matched.name,
    kafka_topic.tenant_revenue_hourly.name,
    kafka_topic.dlq_deserialization.name,
    kafka_topic.ledger_dlq.name,
    kafka_topic.sync_runs_cdc.name,
    kafka_topic.sync_run_payloads_cdc.name,
    kafka_topic.sync_runs_enriched.name,
  ]
}

output "total_partitions" {
  description = "Total number of partitions across all topics"
  value = (
    kafka_topic.tx_events.partitions +
    kafka_topic.ledger_balances.partitions +
    kafka_topic.fraud_alerts.partitions +
    kafka_topic.payment_commands.partitions +
    kafka_topic.payment_events.partitions +
    kafka_topic.payment_dlq.partitions +
    kafka_topic.payment_matched.partitions +
    kafka_topic.tenant_revenue_hourly.partitions +
    kafka_topic.dlq_deserialization.partitions +
    kafka_topic.ledger_dlq.partitions +
    kafka_topic.sync_runs_cdc.partitions +
    kafka_topic.sync_run_payloads_cdc.partitions +
    kafka_topic.sync_runs_enriched.partitions
  )
}

