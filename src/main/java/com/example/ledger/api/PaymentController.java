package com.example.ledger.api;

import com.example.ledger.avro.PaymentCommandType;
import com.example.ledger.avro.PaymentEvent;
import com.example.ledger.model.PaymentRequest;
import com.example.ledger.model.PaymentState;
import com.example.ledger.streams.LedgerQueryService;
import com.example.ledger.streams.Topics;
import org.springframework.http.HttpStatus;
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
 *   <li>{@code POST /api/payments} — submit a payment lifecycle command</li>
 *   <li>{@code GET  /api/payments/{tenantId}/{paymentId}} — interactive query on payment state store</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final LedgerQueryService queryService;

    public PaymentController(KafkaTemplate<String, Object> kafkaTemplate,
                             LedgerQueryService queryService) {
        this.kafkaTemplate = kafkaTemplate;
        this.queryService = queryService;
    }

    @PostMapping
    public ResponseEntity<Void> submitCommand(@RequestBody PaymentRequest request) {
        PaymentEvent event = PaymentEvent.newBuilder()
                .setPaymentId(request.paymentId())
                .setTenantId(request.tenantId())
                .setAccountId(request.accountId())
                .setAmount(request.amount())
                .setCurrency(request.currency() == null || request.currency().isBlank()
                        ? "USD" : request.currency())
                .setCommandType(PaymentCommandType.valueOf(request.commandType()))
                .setCorrelationId(request.correlationId())
                .setEventTime(request.eventTime() > 0 ? request.eventTime() : System.currentTimeMillis())
                .build();

        String key = request.tenantId() + "|" + request.paymentId();
        kafkaTemplate.send(Topics.PAYMENT_COMMANDS, key, event);
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }

    @GetMapping("/{tenantId}/{paymentId}")
    public ResponseEntity<PaymentState> getPaymentState(@PathVariable String tenantId,
                                                        @PathVariable String paymentId) {
        return queryService.findPaymentState(tenantId, paymentId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}

