package com.example.ledger.api;

import com.example.ledger.model.FraudStateView;
import com.example.ledger.streams.LedgerQueryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/fraud")
public class FraudQueryController {

    private final LedgerQueryService queryService;

    public FraudQueryController(LedgerQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/{tenantId}/{accountId}")
    public ResponseEntity<FraudStateView> getFraudState(@PathVariable String tenantId, @PathVariable String accountId) {
        return queryService.findFraudState(tenantId, accountId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}

