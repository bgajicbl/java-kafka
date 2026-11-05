package com.example.ledger.api;

import com.example.ledger.avro.TransactionEvent;
import com.example.ledger.model.Money;
import com.example.ledger.model.TransactionRequest;
import com.example.ledger.security.TenantAccess;
import com.example.ledger.streams.Topics;
import jakarta.validation.Valid;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Ingests ledger transactions. Responds {@code 202} only after the broker has acknowledged
 * the write ({@code acks=all}); a failed or timed-out send surfaces as {@code 503} so the
 * client retries — safe, because the ledger de-duplicates on {@code eventId}.
 */
@RestController
@RequestMapping("/api/transactions")
public class TransactionIngestController {

    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;
    private final TenantAccess tenantAccess;

    public TransactionIngestController(KafkaTemplate<String, TransactionEvent> kafkaTemplate,
                                       TenantAccess tenantAccess) {
        this.kafkaTemplate = kafkaTemplate;
        this.tenantAccess = tenantAccess;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<Void>> publish(@Valid @RequestBody TransactionRequest request) {
        tenantAccess.require(request.tenantId());

        TransactionEvent event = TransactionEvent.newBuilder()
                .setEventId(request.eventId())
                .setTenantId(request.tenantId())
                .setAccountId(request.accountId())
                .setAmount(Money.normalizePositive(request.amount()))
                .setDirection(request.direction())
                .setCurrency(Money.normalizeCurrency(request.currency()))
                .setEventTime(request.eventTime())
                .build();

        String key = request.tenantId() + "|" + request.accountId();
        return kafkaTemplate.send(Topics.TRANSACTION_EVENTS, key, event)
                .thenApply(result -> ResponseEntity.accepted().build());
    }
}
