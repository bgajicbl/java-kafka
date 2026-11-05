package com.example.ledger.api;

import com.example.ledger.avro.PaymentEvent;
import com.example.ledger.model.Identifiers;
import com.example.ledger.model.Money;
import com.example.ledger.model.PaymentRequest;
import com.example.ledger.model.PaymentState;
import com.example.ledger.security.TenantAccess;
import com.example.ledger.streams.LedgerQueryService;
import com.example.ledger.streams.Topics;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for the payment orchestration subsystem.
 *
 * <ul>
 *   <li>{@code POST /api/payments} — submit a payment lifecycle command; {@code 202} once the
 *       broker has acknowledged it</li>
 *   <li>{@code GET  /api/payments/{tenantId}/{paymentId}} — interactive query on payment state store</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final LedgerQueryService queryService;
    private final TenantAccess tenantAccess;
    private final Clock clock;

    public PaymentController(KafkaTemplate<String, Object> kafkaTemplate,
                             LedgerQueryService queryService,
                             TenantAccess tenantAccess,
                             Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.queryService = queryService;
        this.tenantAccess = tenantAccess;
        this.clock = clock;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<Void>> submitCommand(@Valid @RequestBody PaymentRequest request) {
        tenantAccess.require(request.tenantId());

        PaymentEvent event = PaymentEvent.newBuilder()
                .setPaymentId(request.paymentId())
                .setTenantId(request.tenantId())
                .setAccountId(request.accountId())
                .setAmount(Money.normalizePositive(request.amount()))
                .setCurrency(Money.normalizeCurrency(request.currency()))
                .setCommandType(request.commandType())
                .setCorrelationId(request.correlationId())
                .setEventTime(request.eventTime() > 0 ? request.eventTime() : clock.millis())
                .build();

        String key = request.tenantId() + "|" + request.paymentId();
        return kafkaTemplate.send(Topics.PAYMENT_COMMANDS, key, event)
                .thenApply(result -> ResponseEntity.accepted().build());
    }

    @GetMapping("/{tenantId}/{paymentId}")
    public ResponseEntity<PaymentState> getPaymentState(
            @PathVariable @Pattern(regexp = Identifiers.PATTERN) String tenantId,
            @PathVariable @Pattern(regexp = Identifiers.PATTERN) String paymentId) {
        tenantAccess.require(tenantId);
        return queryService.findPaymentState(tenantId, paymentId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
