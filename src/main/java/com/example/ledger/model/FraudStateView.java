package com.example.ledger.model;

public record FraudStateView(String tenantId, String accountId, int eventsInWindow, long windowStartMs, long lastEventTime) {
}

