package com.example.ledger.model;

import java.util.List;

public record SyncRunMergedRecord(
        String syncRunId,
        SyncRunRecord syncRun,
        List<SyncRunPayloadRecord> payloads,
        int payloadCount
) {
}

