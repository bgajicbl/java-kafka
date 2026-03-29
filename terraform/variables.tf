variable "bootstrap_servers" {
  description = "Kafka bootstrap servers (comma-separated for multiple brokers)"
  type        = string
  default     = "localhost:9092"
}

variable "replication_factor" {
  description = "Default replication factor for topics"
  type        = number
  default     = 1
}

variable "default_partitions" {
  description = "Default number of partitions for topics"
  type        = number
  default     = 6
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

