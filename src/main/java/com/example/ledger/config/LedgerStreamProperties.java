package com.example.ledger.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.ledger")
public class LedgerStreamProperties {

    private double singleTransactionAlertThreshold = 10_000.0;
    private long burstWindowMs = 60_000L;
    private int burstCountThreshold = 5;

    public double getSingleTransactionAlertThreshold() {
        return singleTransactionAlertThreshold;
    }

    public void setSingleTransactionAlertThreshold(double singleTransactionAlertThreshold) {
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
}

