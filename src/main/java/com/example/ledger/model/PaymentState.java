package com.example.ledger.model;

import java.math.BigDecimal;

/**
 * Represents the current state of a payment in the orchestration state machine.
 * Stored in a KTable state store (RocksDB) and serialized as JSON.
 *
 * <p>State transitions:
 * <pre>
 *   null/UNKNOWN → INITIATED → AUTHORIZED → CAPTURED → SETTLED
 *                       ↓            ↓           ↓
 *                   REVERSED     REVERSED     REVERSED
 * </pre>
 *
 * <p>Invalid transitions set {@code failReason} without changing the status,
 * allowing the DLQ branch to pick them up while keeping the state store clean.
 */
public record PaymentState(
        String paymentId,
        String tenantId,
        String accountId,
        BigDecimal amount,
        String currency,
        String status,
        String failReason,
        long initiatedAt,
        long authorizedAt,
        long capturedAt,
        long settledAt,
        long reversedAt,
        long lastEventTime
) {
    /** Sentinel value for a brand-new payment that has never been seen. */
    public static PaymentState initial() {
        return new PaymentState(null, null, null, Money.ZERO, Money.DEFAULT_CURRENCY, "UNKNOWN", null,
                0, 0, 0, 0, 0, 0);
    }

    /** Create a copy with the fail reason cleared (after a successful transition). */
    public PaymentState clearFailure() {
        return new PaymentState(paymentId, tenantId, accountId, amount, currency,
                status, null, initiatedAt, authorizedAt, capturedAt,
                settledAt, reversedAt, lastEventTime);
    }
}

