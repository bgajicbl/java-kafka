package com.example.ledger.model;

public record SyncRunPayloadRecord(
        String syncRunPayloadId,
        String syncRunId,
        String transactionId,
        String orderNumber,
        Double orderAmount,
        String createdAt,
        String updatedAt,
        String venueId,
        String customerId,
        String lineItems,
        String orderDate,
        Integer totalItems,
        String payments,
        String note,
        String currencyCode,
        String currencySymbol,
        String visitAlias
) {
}

