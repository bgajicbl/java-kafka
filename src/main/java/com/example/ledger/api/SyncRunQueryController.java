package com.example.ledger.api;

import com.example.ledger.model.SyncRunMergedRecord;
import com.example.ledger.streams.LedgerQueryService;
import com.example.ledger.model.Identifiers;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * CDC-merged sync runs. Sync runs are not tenant-scoped in the source schema, so any
 * authenticated caller may read them.
 */
@RestController
@RequestMapping("/api/sync-runs")
public class SyncRunQueryController {

    private final LedgerQueryService queryService;

    public SyncRunQueryController(LedgerQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/{syncRunId}")
    public ResponseEntity<SyncRunMergedRecord> getSyncRun(@PathVariable @Pattern(regexp = Identifiers.PATTERN) String syncRunId) {
        return queryService.findSyncRun(syncRunId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}

