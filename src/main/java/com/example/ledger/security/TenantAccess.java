package com.example.ledger.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;

/**
 * Enforces tenant isolation: a caller may only read or write data for the tenant named in
 * the {@value #TENANT_CLAIM} claim of its access token. The tenant in a path or request body
 * is never trusted on its own.
 */
@Component
public class TenantAccess {

    public static final String TENANT_CLAIM = "tenant_id";

    private final boolean securityEnabled;

    public TenantAccess(@Value("${app.security.enabled:true}") boolean securityEnabled) {
        this.securityEnabled = securityEnabled;
    }

    /** @throws AccessDeniedException (→ 403) if the caller's token is not scoped to {@code tenantId} */
    public void require(String tenantId) {
        if (!securityEnabled) {
            return;
        }
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication instanceof JwtAuthenticationToken jwt
                && tenantId.equals(jwt.getToken().getClaimAsString(TENANT_CLAIM))) {
            return;
        }
        throw new AccessDeniedException("Caller is not authorised for tenant " + tenantId);
    }
}
