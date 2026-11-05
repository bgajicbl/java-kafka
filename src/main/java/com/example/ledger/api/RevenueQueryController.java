package com.example.ledger.api;

import com.example.ledger.model.Money;
import com.example.ledger.model.RevenueWindowEntry;
import com.example.ledger.streams.LedgerQueryService;
import java.time.Instant;
import java.util.List;
import com.example.ledger.model.Identifiers;
import com.example.ledger.security.TenantAccess;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for windowed revenue analytics.
 *
 * <p>{@code GET /api/revenue/{tenantId}?currency=USD&from=...&to=...} — queries the
 * hourly tumbling-window state store for a tenant's revenue in one currency within a
 * time range. Revenue is aggregated per currency; amounts in different currencies are
 * never summed.
 */
@RestController
@RequestMapping("/api/revenue")
public class RevenueQueryController {

    private final LedgerQueryService queryService;
    private final TenantAccess tenantAccess;

    public RevenueQueryController(LedgerQueryService queryService, TenantAccess tenantAccess) {
        this.queryService = queryService;
        this.tenantAccess = tenantAccess;
    }

    @GetMapping("/{tenantId}")
    public ResponseEntity<List<RevenueWindowEntry>> getHourlyRevenue(
            @PathVariable @Pattern(regexp = Identifiers.PATTERN) String tenantId,
            @RequestParam String currency,
            @RequestParam(defaultValue = "0") long from,
            @RequestParam(defaultValue = "0") long to) {
        tenantAccess.require(tenantId);

        Instant fromInstant = from > 0 ? Instant.ofEpochMilli(from) : Instant.now().minusSeconds(86400);
        Instant toInstant = to > 0 ? Instant.ofEpochMilli(to) : Instant.now();

        List<RevenueWindowEntry> results = queryService.findHourlyRevenue(
                tenantId, Money.normalizeCurrency(currency), fromInstant, toInstant);
        return ResponseEntity.ok(results);
    }
}

