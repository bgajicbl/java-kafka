package com.example.ledger.api;

import com.example.ledger.model.RevenueWindowEntry;
import com.example.ledger.streams.LedgerQueryService;
import java.time.Instant;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for windowed revenue analytics.
 *
 * <p>{@code GET /api/revenue/{tenantId}?from=...&to=...} — queries the
 * hourly tumbling-window state store for a tenant's revenue within a time range.
 */
@RestController
@RequestMapping("/api/revenue")
public class RevenueQueryController {

    private final LedgerQueryService queryService;

    public RevenueQueryController(LedgerQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/{tenantId}")
    public ResponseEntity<List<RevenueWindowEntry>> getHourlyRevenue(
            @PathVariable String tenantId,
            @RequestParam(defaultValue = "0") long from,
            @RequestParam(defaultValue = "0") long to) {

        Instant fromInstant = from > 0 ? Instant.ofEpochMilli(from) : Instant.now().minusSeconds(86400);
        Instant toInstant = to > 0 ? Instant.ofEpochMilli(to) : Instant.now();

        List<RevenueWindowEntry> results = queryService.findHourlyRevenue(tenantId, fromInstant, toInstant);
        return ResponseEntity.ok(results);
    }
}

