package com.example.ledger.api;

import com.example.ledger.model.FraudStateView;
import com.example.ledger.streams.LedgerQueryService;
import com.example.ledger.model.Identifiers;
import com.example.ledger.security.TenantAccess;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/fraud")
public class FraudQueryController {

    private final LedgerQueryService queryService;
    private final TenantAccess tenantAccess;

    public FraudQueryController(LedgerQueryService queryService, TenantAccess tenantAccess) {
        this.queryService = queryService;
        this.tenantAccess = tenantAccess;
    }

    @GetMapping("/{tenantId}/{accountId}")
    public ResponseEntity<FraudStateView> getFraudState(@PathVariable @Pattern(regexp = Identifiers.PATTERN) String tenantId,
            @PathVariable @Pattern(regexp = Identifiers.PATTERN) String accountId) {
        tenantAccess.require(tenantId);
        return queryService.findFraudState(tenantId, accountId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}

