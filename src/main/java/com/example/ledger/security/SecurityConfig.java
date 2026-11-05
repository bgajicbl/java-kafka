package com.example.ledger.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Stateless OAuth2 resource server: every API call needs a bearer JWT, validated against the
 * issuer in {@code spring.security.oauth2.resourceserver.jwt.issuer-uri}. Tenant scoping is
 * enforced per request by {@link TenantAccess}.
 *
 * <p>Secure unless {@code app.security.enabled=false}, which the local docker-compose setup
 * uses so the README's curl examples work without an identity provider.
 */
@Configuration
public class SecurityConfig {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);

    private static final String[] PUBLIC_ENDPOINTS = {
            "/actuator/health/**", "/actuator/info", "/actuator/prometheus"
    };

    @Bean
    @ConditionalOnProperty(name = "app.security.enabled", havingValue = "true", matchIfMissing = true)
    public SecurityFilterChain jwtSecuredApi(HttpSecurity http) throws Exception {
        return statelessApi(http)
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(PUBLIC_ENDPOINTS).permitAll()
                        .anyRequest().authenticated())
                .oauth2ResourceServer(oauth2 -> oauth2.jwt(Customizer.withDefaults()))
                .build();
    }

    @Bean
    @ConditionalOnProperty(name = "app.security.enabled", havingValue = "false")
    public SecurityFilterChain unsecuredApi(HttpSecurity http) throws Exception {
        log.warn("app.security.enabled=false: API authentication and tenant isolation are DISABLED (local dev only)");
        return statelessApi(http)
                .authorizeHttpRequests(auth -> auth.anyRequest().permitAll())
                .build();
    }

    private static HttpSecurity statelessApi(HttpSecurity http) throws Exception {
        // Bearer tokens, no cookies or sessions → CSRF does not apply
        return http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS));
    }
}
