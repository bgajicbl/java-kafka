package com.example.ledger.model;

public record BalanceView(String tenantId, String accountId, double balance, String currency, long lastEventTime) {
}

