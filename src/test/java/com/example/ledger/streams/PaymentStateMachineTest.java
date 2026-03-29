package com.example.ledger.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.ledger.avro.PaymentCommandType;
import com.example.ledger.avro.PaymentEvent;
import com.example.ledger.model.PaymentState;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the pure-function payment state machine — no Kafka infrastructure needed.
 */
class PaymentStateMachineTest {

    @Test
    void validFullLifecycle() {
        PaymentState state = PaymentState.initial();

        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.INITIATE, 1000));
        assertThat(state.status()).isEqualTo("INITIATED");
        assertThat(state.failReason()).isNull();

        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.AUTHORIZE, 2000));
        assertThat(state.status()).isEqualTo("AUTHORIZED");

        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.CAPTURE, 3000));
        assertThat(state.status()).isEqualTo("CAPTURED");

        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.SETTLE, 4000));
        assertThat(state.status()).isEqualTo("SETTLED");
        assertThat(state.settledAt()).isEqualTo(4000L);
    }

    @Test
    void reverseFromInitiated() {
        PaymentState state = PaymentStateMachine.transition(PaymentState.initial(),
                cmd(PaymentCommandType.INITIATE, 1000));
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.REVERSE, 2000));
        assertThat(state.status()).isEqualTo("REVERSED");
    }

    @Test
    void reverseFromAuthorized() {
        PaymentState state = PaymentState.initial();
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.INITIATE, 1000));
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.AUTHORIZE, 2000));
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.REVERSE, 3000));
        assertThat(state.status()).isEqualTo("REVERSED");
    }

    @Test
    void reverseFromCaptured() {
        PaymentState state = PaymentState.initial();
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.INITIATE, 1000));
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.AUTHORIZE, 2000));
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.CAPTURE, 3000));
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.REVERSE, 4000));
        assertThat(state.status()).isEqualTo("REVERSED");
    }

    @Test
    void cannotReverseSettled() {
        PaymentState state = PaymentState.initial();
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.INITIATE, 1000));
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.AUTHORIZE, 2000));
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.CAPTURE, 3000));
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.SETTLE, 4000));
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.REVERSE, 5000));

        assertThat(state.status()).isEqualTo("SETTLED");
        assertThat(state.failReason()).contains("Cannot REVERSE");
    }

    @Test
    void cannotDoubleInitiate() {
        PaymentState state = PaymentStateMachine.transition(PaymentState.initial(),
                cmd(PaymentCommandType.INITIATE, 1000));
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.INITIATE, 2000));

        assertThat(state.status()).isEqualTo("INITIATED");
        assertThat(state.failReason()).contains("Cannot INITIATE");
    }

    @Test
    void cannotSettleFromInitiated() {
        PaymentState state = PaymentStateMachine.transition(PaymentState.initial(),
                cmd(PaymentCommandType.INITIATE, 1000));
        state = PaymentStateMachine.transition(state, cmd(PaymentCommandType.SETTLE, 2000));

        assertThat(state.status()).isEqualTo("INITIATED");
        assertThat(state.failReason()).contains("Cannot SETTLE");
    }

    private PaymentEvent cmd(PaymentCommandType type, long eventTime) {
        return PaymentEvent.newBuilder()
                .setPaymentId("pay-1")
                .setTenantId("tenant-a")
                .setAccountId("acct-1")
                .setAmount(100.0)
                .setCurrency("USD")
                .setCommandType(type)
                .setCorrelationId(null)
                .setEventTime(eventTime)
                .build();
    }
}

