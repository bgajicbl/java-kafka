package com.example.ledger.model;

import com.example.ledger.avro.TxDirection;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;

/**
 * REST API request body for a ledger transaction. The amount is always positive;
 * {@code direction} carries the sign.
 */
public record TransactionRequest(
        @NotNull @Pattern(regexp = Identifiers.PATTERN) String eventId,
        @NotNull @Pattern(regexp = Identifiers.PATTERN) String tenantId,
        @NotNull @Pattern(regexp = Identifiers.PATTERN) String accountId,
        @NotNull @Positive @Digits(integer = Money.PRECISION - Money.SCALE, fraction = Money.SCALE) BigDecimal amount,
        @NotNull TxDirection direction,
        @Pattern(regexp = "[A-Za-z]{3}") String currency,
        @Positive long eventTime
) {
}
