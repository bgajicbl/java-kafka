package com.example.ledger.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.ledger.avro.TransactionEvent;
import com.example.ledger.security.SecurityConfig;
import com.example.ledger.security.TenantAccess;
import com.example.ledger.streams.LedgerQueryService;
import com.example.ledger.streams.Topics;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.JwtRequestPostProcessor;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@WebMvcTest({TransactionIngestController.class, PaymentController.class, LedgerQueryController.class})
@Import({SecurityConfig.class, TenantAccess.class})
@TestPropertySource(properties = "app.security.enabled=true")
class ApiSecurityAndValidationTest {

    private static final String VALID_TX = """
            {"eventId":"e1","tenantId":"tenant-a","accountId":"acct-1","amount":100.25,
             "direction":"CREDIT","currency":"usd","eventTime":1710000000000}
            """;

    @Autowired MockMvc mvc;

    @MockBean KafkaTemplate<String, TransactionEvent> transactionTemplate;
    @MockBean KafkaTemplate<String, Object> paymentTemplate;
    @MockBean LedgerQueryService queryService;
    @MockBean JwtDecoder jwtDecoder;
    @MockBean Clock clock;

    @Test
    void rejectsUnauthenticatedCalls() throws Exception {
        mvc.perform(post("/api/transactions").contentType(MediaType.APPLICATION_JSON).content(VALID_TX))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void rejectsWritesForAnotherTenant() throws Exception {
        mvc.perform(post("/api/transactions").with(tenant("tenant-b"))
                        .contentType(MediaType.APPLICATION_JSON).content(VALID_TX))
                .andExpect(status().isForbidden());
        verify(transactionTemplate, never()).send(anyString(), anyString(), any(TransactionEvent.class));
    }

    @Test
    void rejectsReadsForAnotherTenant() throws Exception {
        mvc.perform(get("/api/ledger/tenant-a/acct-1").with(tenant("tenant-b")))
                .andExpect(status().isForbidden());
        verifyNoInteractions(queryService);
    }

    @Test
    void acceptsOnlyAfterBrokerAcknowledges() throws Exception {
        when(transactionTemplate.send(eq(Topics.TRANSACTION_EVENTS), eq("tenant-a|acct-1"), any(TransactionEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(new SendResult<>(null, null)));

        MvcResult pending = mvc.perform(post("/api/transactions").with(tenant("tenant-a"))
                        .contentType(MediaType.APPLICATION_JSON).content(VALID_TX))
                .andExpect(request().asyncStarted())
                .andReturn();

        mvc.perform(asyncDispatch(pending)).andExpect(status().isAccepted());
    }

    @Test
    void reportsUnacknowledgedWriteAsRetryable503() throws Exception {
        when(transactionTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(new KafkaProducerException(
                        new ProducerRecord<>(Topics.TRANSACTION_EVENTS, "k"), "timed out", new RuntimeException())));

        MvcResult pending = mvc.perform(post("/api/transactions").with(tenant("tenant-a"))
                        .contentType(MediaType.APPLICATION_JSON).content(VALID_TX))
                .andReturn();

        mvc.perform(asyncDispatch(pending))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().exists("Retry-After"));
    }

    @Test
    void rejectsInvalidBodies() throws Exception {
        String negativeAmount = VALID_TX.replace("100.25", "-5");
        String delimiterInId = VALID_TX.replace("\"acct-1\"", "\"acct|1\"");
        String tooPrecise = VALID_TX.replace("100.25", "0.00001");

        for (String body : new String[]{negativeAmount, delimiterInId, tooPrecise, "{}"}) {
            mvc.perform(post("/api/transactions").with(tenant("tenant-a"))
                            .contentType(MediaType.APPLICATION_JSON).content(body))
                    .andExpect(status().isBadRequest());
        }
        verify(transactionTemplate, never()).send(anyString(), anyString(), any(TransactionEvent.class));
    }

    @Test
    void rejectsUnknownPaymentCommandAs400() throws Exception {
        mvc.perform(post("/api/payments").with(tenant("tenant-a"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"paymentId":"p1","tenantId":"tenant-a","accountId":"acct-1",
                                 "amount":10,"commandType":"REFUND"}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void servesOwnTenantQueries() throws Exception {
        when(queryService.findBalance("tenant-a", "acct-1")).thenReturn(Optional.empty());

        mvc.perform(get("/api/ledger/tenant-a/acct-1").with(tenant("tenant-a")))
                .andExpect(status().isNotFound());
    }

    private static JwtRequestPostProcessor tenant(String tenantId) {
        return jwt().jwt(token -> token.claim(TenantAccess.TENANT_CLAIM, tenantId));
    }
}
