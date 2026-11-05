package com.example.ledger.streams;

/**
 * A state store cannot answer right now — Kafka Streams is starting, rebalancing or
 * restoring. Transient by nature; the API maps it to {@code 503 Service Unavailable}.
 */
public class StoreUnavailableException extends RuntimeException {

    public StoreUnavailableException(String message) {
        super(message);
    }

    public StoreUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
