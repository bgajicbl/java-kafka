package com.example.ledger.config;

import java.math.BigDecimal;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.ledger")
public class LedgerStreamProperties {

    private BigDecimal singleTransactionAlertThreshold = BigDecimal.valueOf(10_000);
    private long burstWindowMs = 60_000L;
    private int burstCountThreshold = 5;
    /** How long an eventId is remembered for de-duplication; should cover the input topic retention. */
    private Duration dedupWindow = Duration.ofDays(7);

    public BigDecimal getSingleTransactionAlertThreshold() {
        return singleTransactionAlertThreshold;
    }

    public void setSingleTransactionAlertThreshold(BigDecimal singleTransactionAlertThreshold) {
        this.singleTransactionAlertThreshold = singleTransactionAlertThreshold;
    }

    public long getBurstWindowMs() {
        return burstWindowMs;
    }

    public void setBurstWindowMs(long burstWindowMs) {
        this.burstWindowMs = burstWindowMs;
    }

    public int getBurstCountThreshold() {
        return burstCountThreshold;
    }

    public void setBurstCountThreshold(int burstCountThreshold) {
        this.burstCountThreshold = burstCountThreshold;
    }

    public Duration getDedupWindow() {
        return dedupWindow;
    }

    public void setDedupWindow(Duration dedupWindow) {
        this.dedupWindow = dedupWindow;
    }
}

