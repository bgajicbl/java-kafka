package com.example.ledger.model;

import java.math.BigDecimal;

/**
 * A single entry from a windowed revenue aggregation.
 * Returned by the revenue interactive query endpoint.
 */
public record RevenueWindowEntry(
        long windowStart,
        long windowEnd,
        BigDecimal totalAmount,
        long transactionCount,
        String currency
) {
}

