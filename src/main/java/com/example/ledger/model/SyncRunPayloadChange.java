package com.example.ledger.model;

public record SyncRunPayloadChange(
        String syncRunId,
        String syncRunPayloadId,
        SyncRunPayloadRecord payload,
        boolean deleted
) {
}

