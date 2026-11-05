package com.example.ledger.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class MoneyTest {

    @Test
    void normalizesToFixedScale() {
        assertThat(Money.normalize(new BigDecimal("12.5"))).isEqualTo(new BigDecimal("12.5000"));
    }

    @Test
    void rejectsSubMinorUnitPrecisionInsteadOfRounding() {
        assertThatThrownBy(() -> Money.normalize(new BigDecimal("0.00001")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("decimal places");
    }

    @Test
    void rejectsAmountsBeyondSchemaPrecision() {
        assertThatThrownBy(() -> Money.normalize(new BigDecimal("1000000000000000")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("significant digits");
    }

    @Test
    void requiresPositiveAmountsAtIngress() {
        assertThatThrownBy(() -> Money.normalizePositive(BigDecimal.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Money.normalizePositive(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validatesIsoCurrencyCodes() {
        assertThat(Money.normalizeCurrency(" eur ")).isEqualTo("EUR");
        assertThat(Money.normalizeCurrency(null)).isEqualTo("USD");
        assertThatThrownBy(() -> Money.normalizeCurrency("XYZ1"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
