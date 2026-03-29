package com.example.ledger.api;

import com.example.ledger.model.BalanceView;
import com.example.ledger.streams.LedgerQueryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/ledger")
public class LedgerQueryController {

    private final LedgerQueryService queryService;

    public LedgerQueryController(LedgerQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/{tenantId}/{accountId}")
    public ResponseEntity<BalanceView> getBalance(@PathVariable String tenantId, @PathVariable String accountId) {
        return queryService.findBalance(tenantId, accountId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}

