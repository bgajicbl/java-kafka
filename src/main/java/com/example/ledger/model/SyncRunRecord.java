package com.example.ledger.model;

public record SyncRunRecord(
        String syncRunId,
        String startedAt,
        String finishedAt,
        String status,
        String createdAt,
        String createdBy,
        String updatedAt,
        String updatedBy,
        String syncJobId,
        String jobInfo
) {
}

