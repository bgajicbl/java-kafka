package com.example.ledger.streams;

import com.example.ledger.avro.PaymentCommandType;
import com.example.ledger.avro.PaymentEvent;
import com.example.ledger.model.PaymentState;

/**
 * Pure-function state machine for payment lifecycle transitions.
 *
 * <pre>
 *   UNKNOWN ──INITIATE──▶ INITIATED
 *                              │
 *                         AUTHORIZE
 *                              ▼
 *                          AUTHORIZED ──REVERSE──▶ REVERSED
 *                              │
 *                           CAPTURE
 *                              ▼
 *                          CAPTURED ──REVERSE──▶ REVERSED
 *                              │
 *                           SETTLE
 *                              ▼
 *                          SETTLED (terminal)
 * </pre>
 *
 * Invalid transitions leave the state unchanged but set {@code failReason}
 * so the downstream DLQ branch can pick them up.
 */
public final class PaymentStateMachine {

    private PaymentStateMachine() {
    }

    /**
     * Apply a command to the current state, returning the new state.
     *
     * @param current the current payment state (may be {@link PaymentState#initial()})
     * @param command the incoming payment command
     * @return updated state — check {@link PaymentState#failReason()} for invalid transitions
     */
    public static PaymentState transition(PaymentState current, PaymentEvent command) {
        PaymentCommandType type = command.getCommandType();
        String currentStatus = current.status();

        return switch (type) {
            case INITIATE -> {
                if ("UNKNOWN".equals(currentStatus)) {
                    yield new PaymentState(
                            command.getPaymentId(),
                            command.getTenantId(),
                            command.getAccountId(),
                            command.getAmount(),
                            command.getCurrency(),
                            "INITIATED",
                            null,
                            command.getEventTime(), 0, 0, 0, 0,
                            command.getEventTime()
                    );
                }
                yield reject(current, command, "Cannot INITIATE: already " + currentStatus);
            }

            case AUTHORIZE -> {
                if ("INITIATED".equals(currentStatus)) {
                    yield new PaymentState(
                            current.paymentId(),
                            current.tenantId(),
                            current.accountId(),
                            current.amount(),
                            current.currency(),
                            "AUTHORIZED",
                            null,
                            current.initiatedAt(),
                            command.getEventTime(), 0, 0, 0,
                            command.getEventTime()
                    );
                }
                yield reject(current, command, "Cannot AUTHORIZE: current status is " + currentStatus);
            }

            case CAPTURE -> {
                if ("AUTHORIZED".equals(currentStatus)) {
                    yield new PaymentState(
                            current.paymentId(),
                            current.tenantId(),
                            current.accountId(),
                            current.amount(),
                            current.currency(),
                            "CAPTURED",
                            null,
                            current.initiatedAt(),
                            current.authorizedAt(),
                            command.getEventTime(), 0, 0,
                            command.getEventTime()
                    );
                }
                yield reject(current, command, "Cannot CAPTURE: current status is " + currentStatus);
            }

            case SETTLE -> {
                if ("CAPTURED".equals(currentStatus)) {
                    yield new PaymentState(
                            current.paymentId(),
                            current.tenantId(),
                            current.accountId(),
                            current.amount(),
                            current.currency(),
                            "SETTLED",
                            null,
                            current.initiatedAt(),
                            current.authorizedAt(),
                            current.capturedAt(),
                            command.getEventTime(), 0,
                            command.getEventTime()
                    );
                }
                yield reject(current, command, "Cannot SETTLE: current status is " + currentStatus);
            }

            case REVERSE -> {
                if ("INITIATED".equals(currentStatus)
                        || "AUTHORIZED".equals(currentStatus)
                        || "CAPTURED".equals(currentStatus)) {
                    yield new PaymentState(
                            current.paymentId(),
                            current.tenantId(),
                            current.accountId(),
                            current.amount(),
                            current.currency(),
                            "REVERSED",
                            null,
                            current.initiatedAt(),
                            current.authorizedAt(),
                            current.capturedAt(),
                            0,
                            command.getEventTime(),
                            command.getEventTime()
                    );
                }
                yield reject(current, command, "Cannot REVERSE: current status is " + currentStatus);
            }
        };
    }

    private static PaymentState reject(PaymentState current, PaymentEvent command, String reason) {
        // Status remains unchanged; failReason is set for DLQ routing
        return new PaymentState(
                current.paymentId() != null ? current.paymentId() : command.getPaymentId(),
                current.tenantId() != null ? current.tenantId() : command.getTenantId(),
                current.accountId() != null ? current.accountId() : command.getAccountId(),
                current.amount() > 0 ? current.amount() : command.getAmount(),
                current.currency(),
                current.status(),
                reason,
                current.initiatedAt(),
                current.authorizedAt(),
                current.capturedAt(),
                current.settledAt(),
                current.reversedAt(),
                command.getEventTime()
        );
    }
}

