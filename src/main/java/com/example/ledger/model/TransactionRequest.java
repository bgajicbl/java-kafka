package com.example.ledger.model;

import com.example.ledger.avro.TxDirection;

public record TransactionRequest(
        String eventId,
        String tenantId,
        String accountId,
        double amount,
        TxDirection direction,
        String currency,
        long eventTime
) {
}

