package com.example.ledger.model;

import com.example.ledger.avro.PaymentCommandType;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;

/**
 * REST API request body for submitting a payment command.
 * Maps to an Avro {@code PaymentEvent} before being produced to Kafka.
 * An {@code eventTime} of 0 means "now".
 */
public record PaymentRequest(
        @NotNull @Pattern(regexp = Identifiers.PATTERN) String paymentId,
        @NotNull @Pattern(regexp = Identifiers.PATTERN) String tenantId,
        @NotNull @Pattern(regexp = Identifiers.PATTERN) String accountId,
        @NotNull @Positive @Digits(integer = Money.PRECISION - Money.SCALE, fraction = Money.SCALE) BigDecimal amount,
        @Pattern(regexp = "[A-Za-z]{3}") String currency,
        @NotNull PaymentCommandType commandType,
        @Size(max = 128) String correlationId,
        @PositiveOrZero long eventTime
) {
}
