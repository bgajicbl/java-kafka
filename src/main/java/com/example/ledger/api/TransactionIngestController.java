package com.example.ledger.api;

import com.example.ledger.avro.TransactionEvent;
import com.example.ledger.model.TransactionRequest;
import com.example.ledger.streams.Topics;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/transactions")
public class TransactionIngestController {

    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    public TransactionIngestController(KafkaTemplate<String, TransactionEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public ResponseEntity<Void> publish(@RequestBody TransactionRequest request) {
        TransactionEvent event = TransactionEvent.newBuilder()
                .setEventId(request.eventId())
                .setTenantId(request.tenantId())
                .setAccountId(request.accountId())
                .setAmount(request.amount())
                .setDirection(request.direction())
                .setCurrency(request.currency() == null || request.currency().isBlank() ? "USD" : request.currency())
                .setEventTime(request.eventTime())
                .build();

        String key = request.tenantId() + "|" + request.accountId();
        kafkaTemplate.send(Topics.TRANSACTION_EVENTS, key, event);
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }
}

