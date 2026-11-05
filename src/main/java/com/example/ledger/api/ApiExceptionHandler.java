package com.example.ledger.api;

import com.example.ledger.streams.StoreUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.KafkaException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.client.RestClientException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Maps failures to RFC 7807 problem responses. The base class covers Spring MVC errors
 * (bean-validation failures, unreadable bodies such as an unknown {@code commandType}) as 400s.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);
    private static final String RETRY_AFTER_SECONDS = "2";

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail badRequest(IllegalArgumentException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    /** Rebalance or restore in progress — clients should retry shortly. */
    @ExceptionHandler(StoreUnavailableException.class)
    public ResponseEntity<ProblemDetail> storeUnavailable(StoreUnavailableException ex) {
        return serviceUnavailable(ex.getMessage());
    }

    /** The broker did not acknowledge the write; nothing was accepted, so the client should retry. */
    @ExceptionHandler(KafkaException.class)
    public ResponseEntity<ProblemDetail> publishFailed(KafkaException ex) {
        log.warn("Kafka publish failed", ex);
        return serviceUnavailable("Event could not be durably recorded; retry with the same id");
    }

    /** The instance owning the key could not be reached or failed. */
    @ExceptionHandler(RestClientException.class)
    public ProblemDetail remoteQueryFailed(RestClientException ex) {
        log.warn("Forwarded interactive query failed", ex);
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_GATEWAY, "Owning instance did not answer the query");
    }

    private static ResponseEntity<ProblemDetail> serviceUnavailable(String detail) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS)
                .body(ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, detail));
    }
}
