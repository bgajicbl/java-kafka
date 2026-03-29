package com.example.ledger.model;

/**
 * REST API request body for submitting a payment command.
 * Maps to an Avro {@code PaymentEvent} before being produced to Kafka.
 */
public record PaymentRequest(
        String paymentId,
        String tenantId,
        String accountId,
        double amount,
        String currency,
        String commandType,
        String correlationId,
        long eventTime
) {
}

