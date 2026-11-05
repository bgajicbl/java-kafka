package com.example.ledger.model;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Currency;

/**
 * Canonical representation of monetary values across the platform.
 *
 * <p>Every amount is a {@link BigDecimal} with exactly {@link #SCALE} fractional digits,
 * matching the Avro {@code decimal(19,4)} logical type used on all topics. Normalising
 * at the ingress boundary means the Avro serializer never sees a value it cannot encode
 * and arithmetic inside the topologies never rounds.
 */
public final class Money {

    public static final int PRECISION = 19;
    public static final int SCALE = 4;
    public static final BigDecimal ZERO = BigDecimal.ZERO.setScale(SCALE);
    public static final String DEFAULT_CURRENCY = "USD";

    private Money() {
    }

    /**
     * Rescale to {@link #SCALE} without rounding.
     *
     * @throws IllegalArgumentException if the value is null, has more than {@link #SCALE}
     *                                  fractional digits, or exceeds {@link #PRECISION} digits
     */
    public static BigDecimal normalize(BigDecimal amount) {
        if (amount == null) {
            throw new IllegalArgumentException("amount is required");
        }
        BigDecimal scaled;
        try {
            scaled = amount.setScale(SCALE, RoundingMode.UNNECESSARY);
        } catch (ArithmeticException ex) {
            throw new IllegalArgumentException("amount supports at most " + SCALE + " decimal places: " + amount);
        }
        if (scaled.precision() > PRECISION) {
            throw new IllegalArgumentException("amount exceeds " + PRECISION + " significant digits: " + amount);
        }
        return scaled;
    }

    /** Like {@link #normalize} but additionally requires a strictly positive amount. */
    public static BigDecimal normalizePositive(BigDecimal amount) {
        BigDecimal scaled = normalize(amount);
        if (scaled.signum() <= 0) {
            throw new IllegalArgumentException("amount must be positive: " + amount);
        }
        return scaled;
    }

    /** Upper-cases and validates an ISO-4217 code; blank defaults to {@link #DEFAULT_CURRENCY}. */
    public static String normalizeCurrency(String currency) {
        if (currency == null || currency.isBlank()) {
            return DEFAULT_CURRENCY;
        }
        String code = currency.trim().toUpperCase();
        try {
            return Currency.getInstance(code).getCurrencyCode();
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("unknown ISO-4217 currency: " + currency);
        }
    }
}
