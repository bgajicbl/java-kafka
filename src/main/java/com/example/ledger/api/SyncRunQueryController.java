package com.example.ledger.api;

import com.example.ledger.model.SyncRunMergedRecord;
import com.example.ledger.streams.LedgerQueryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/sync-runs")
public class SyncRunQueryController {

    private final LedgerQueryService queryService;

    public SyncRunQueryController(LedgerQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/{syncRunId}")
    public ResponseEntity<SyncRunMergedRecord> getSyncRun(@PathVariable String syncRunId) {
        return queryService.findSyncRun(syncRunId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}

