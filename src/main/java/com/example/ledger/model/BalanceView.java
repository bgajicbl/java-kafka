package com.example.ledger.model;

import java.math.BigDecimal;

public record BalanceView(String tenantId, String accountId, BigDecimal balance, String currency, long lastEventTime) {
}

