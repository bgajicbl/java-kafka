package com.example.ledger.streams;

import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.util.DefaultUriBuilderFactory;
import org.springframework.web.util.UriBuilder;

/**
 * Routes interactive queries to the instance that owns the key.
 *
 * <p>Each instance only holds the state-store partitions assigned to it. With N replicas,
 * a local-only lookup misses for roughly (N-1)/N of keys. This router asks Kafka Streams
 * which host is active for the key (advertised via {@code application.server}) and either
 * answers locally or forwards the HTTP call to the owner.
 *
 * <p>Forwarded calls carry {@link #FORWARDED_HEADER}; the receiver always answers locally,
 * so a stale view of the assignment during a rebalance cannot cause forwarding loops.
 * The caller's {@code Authorization} header is passed through, so the owning instance
 * re-authenticates the original caller and applies the same tenant check.
 * When {@code application.server} is unset (single-instance dev), every query is local.
 */
@Component
public class InteractiveQueryRouter {

    static final String FORWARDED_HEADER = "X-Ledger-IQ-Forwarded";

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    private final RestClient restClient;
    private final HostInfo self;

    public InteractiveQueryRouter(StreamsBuilderFactoryBean streamsBuilderFactoryBean,
                                  RestClient.Builder restClientBuilder,
                                  @Value("${spring.kafka.streams.properties.application.server:}") String applicationServer) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
        this.restClient = restClientBuilder.build();
        this.self = applicationServer.isBlank() ? null : HostInfo.buildFromEndpoint(applicationServer);
    }

    /**
     * Answer a key lookup from whichever instance owns {@code key} in {@code storeName}.
     *
     * @param remotePath builds the path of the equivalent public endpoint on the owning instance
     * @param local      performs the lookup against this instance's store
     */
    public <T> Optional<T> query(String storeName,
                                 String key,
                                 Function<UriBuilder, URI> remotePath,
                                 ParameterizedTypeReference<T> responseType,
                                 Supplier<Optional<T>> local) {
        Optional<HostInfo> owner = remoteOwner(storeName, key);
        if (owner.isEmpty()) {
            return local.get();
        }
        HostInfo host = owner.get();
        URI uri = remotePath.apply(new DefaultUriBuilderFactory("http://" + host.host() + ":" + host.port()).builder());
        try {
            return Optional.ofNullable(restClient.get()
                    .uri(uri)
                    .headers(headers -> {
                        headers.set(FORWARDED_HEADER, "true");
                        String authorization = currentHeader(HttpHeaders.AUTHORIZATION);
                        if (authorization != null) {
                            headers.set(HttpHeaders.AUTHORIZATION, authorization);
                        }
                    })
                    .retrieve()
                    .body(responseType));
        } catch (HttpClientErrorException.NotFound notFound) {
            return Optional.empty();
        }
    }

    /** Local store handle; translates "not queryable yet" into {@link StoreUnavailableException}. */
    public <S> S localStore(String storeName, QueryableStoreType<S> storeType) {
        try {
            return streams().store(StoreQueryParameters.fromNameAndType(storeName, storeType));
        } catch (InvalidStateStoreException ex) {
            throw new StoreUnavailableException("State store " + storeName + " is not queryable yet", ex);
        }
    }

    private Optional<HostInfo> remoteOwner(String storeName, String key) {
        if (self == null || isForwardedRequest()) {
            return Optional.empty();
        }
        KeyQueryMetadata metadata = streams().queryMetadataForKey(storeName, key, Serdes.String().serializer());
        if (metadata == null || KeyQueryMetadata.NOT_AVAILABLE.equals(metadata)) {
            throw new StoreUnavailableException("No active host for " + storeName + " yet (rebalancing?)");
        }
        HostInfo active = metadata.activeHost();
        return self.equals(active) ? Optional.empty() : Optional.of(active);
    }

    private KafkaStreams streams() {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null) {
            throw new StoreUnavailableException("Kafka Streams is not running yet");
        }
        return kafkaStreams;
    }

    private static boolean isForwardedRequest() {
        return "true".equals(currentHeader(FORWARDED_HEADER));
    }

    private static String currentHeader(String name) {
        if (RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attributes) {
            HttpServletRequest request = attributes.getRequest();
            return request.getHeader(name);
        }
        return null;
    }
}
