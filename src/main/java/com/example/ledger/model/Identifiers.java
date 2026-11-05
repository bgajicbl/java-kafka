package com.example.ledger.model;

/**
 * Validation rules for externally supplied identifiers.
 *
 * <p>IDs become parts of composite Kafka keys such as {@code tenantId|accountId}; allowing the
 * {@code |} delimiter (or arbitrary text) would let one tenant's key collide with another's.
 */
public final class Identifiers {

    public static final String PATTERN = "[A-Za-z0-9._-]{1,64}";

    private Identifiers() {
    }
}
