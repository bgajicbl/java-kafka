# ─────────────────────────────────────────────────────────────────
# Terraform IaC for Kafka topic provisioning
#
# This file provisions all Kafka topics used by the ledger-learning
# application with correct partition counts, replication factors,
# and retention policies.
#
# Usage:
#   cd terraform
#   terraform init
#   terraform plan -var="bootstrap_servers=kafka:9092"
#   terraform apply -var="bootstrap_servers=kafka:9092"
# ─────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.5"

  required_providers {
    kafka = {
      source  = "Mongey/kafka"
      version = "~> 0.7"
    }
  }
}

provider "kafka" {
  bootstrap_servers = [var.bootstrap_servers]
}

# ── Ledger & Fraud ────────────────────────────────────────────────

resource "kafka_topic" "tx_events" {
  name               = "tx-events"
  replication_factor = var.replication_factor
  partitions         = var.default_partitions
  config = {
    "retention.ms"  = "604800000" # 7 days
    "cleanup.policy" = "delete"
  }
}

resource "kafka_topic" "ledger_balances" {
  name               = "ledger-balances"
  replication_factor = var.replication_factor
  partitions         = var.default_partitions
  config = {
    "cleanup.policy" = "compact"
  }
}

resource "kafka_topic" "fraud_alerts" {
  name               = "fraud-alerts"
  replication_factor = var.replication_factor
  partitions         = var.default_partitions
  config = {
    "retention.ms" = "2592000000" # 30 days
  }
}

# ── Payment Orchestration ─────────────────────────────────────────

resource "kafka_topic" "payment_commands" {
  name               = "payment-commands"
  replication_factor = var.replication_factor
  partitions         = var.default_partitions
  config = {
    "retention.ms" = "604800000"
  }
}

resource "kafka_topic" "payment_events" {
  name               = "payment-events"
  replication_factor = var.replication_factor
  partitions         = var.default_partitions
  config = {
    "cleanup.policy" = "compact"
  }
}

resource "kafka_topic" "payment_dlq" {
  name               = "payment-dlq"
  replication_factor = var.replication_factor
  partitions         = 3
  config = {
    "retention.ms" = "2592000000" # 30 days
  }
}

resource "kafka_topic" "payment_matched" {
  name               = "payment-matched"
  replication_factor = var.replication_factor
  partitions         = var.default_partitions
  config = {
    "retention.ms" = "604800000"
  }
}

# ── Revenue Analytics ─────────────────────────────────────────────

resource "kafka_topic" "tenant_revenue_hourly" {
  name               = "tenant-revenue-hourly"
  replication_factor = var.replication_factor
  partitions         = var.default_partitions
  config = {
    "retention.ms" = "7776000000" # 90 days
  }
}

# ── Dead Letter Queues ────────────────────────────────────────────

resource "kafka_topic" "dlq_deserialization" {
  name               = "dlq-deserialization-errors"
  replication_factor = var.replication_factor
  partitions         = 3
  config = {
    "retention.ms" = "2592000000"
  }
}

resource "kafka_topic" "dlq_processing" {
  name               = "dlq-processing-errors"
  replication_factor = var.replication_factor
  partitions         = 3
  config = {
    "retention.ms" = "2592000000"
  }
}

# ── CDC (Debezium) ────────────────────────────────────────────────

resource "kafka_topic" "sync_runs_cdc" {
  name               = "sync.public.sync_runs"
  replication_factor = var.replication_factor
  partitions         = var.default_partitions
  config = {
    "retention.ms" = "604800000"
  }
}

resource "kafka_topic" "sync_run_payloads_cdc" {
  name               = "sync.public.sync_run_payloads"
  replication_factor = var.replication_factor
  partitions         = var.default_partitions
  config = {
    "retention.ms" = "604800000"
  }
}

resource "kafka_topic" "sync_runs_enriched" {
  name               = "sync-runs-enriched"
  replication_factor = var.replication_factor
  partitions         = var.default_partitions
  config = {
    "cleanup.policy" = "compact"
  }
}

