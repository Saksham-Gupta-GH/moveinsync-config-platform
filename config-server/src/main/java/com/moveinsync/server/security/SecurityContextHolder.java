package com.moveinsync.server.security;

import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Authentication & Role-Based Access Control (RBAC) with strict Tenant Isolation boundaries.
 * Supported Roles: ADMIN, OPERATOR, EMPLOYEE (read-only service client).
 */
@Component
public class SecurityContextHolder {

    public enum Role {
        ADMIN,
        OPERATOR,
        EMPLOYEE
    }

    public record AuthPrincipal(String apiKey, Role role, String assignedTenantId) {}

    private final Map<String, AuthPrincipal> knownKeys;

    public SecurityContextHolder() {
        // Allow dynamic configuration via environment variables with developer defaults
        String adminKey = System.getenv().getOrDefault("MOVEINSYNC_ADMIN_KEY", "admin-secret-key");
        String operatorKey = System.getenv().getOrDefault("MOVEINSYNC_OPERATOR_KEY", "operator-secret-key");
        String googleKey = System.getenv().getOrDefault("MOVEINSYNC_GOOGLE_KEY", "google-service-key");
        String amazonKey = System.getenv().getOrDefault("MOVEINSYNC_AMAZON_KEY", "amazon-service-key");

        Map<String, AuthPrincipal> keys = new java.util.HashMap<>();
        keys.put(adminKey, new AuthPrincipal(adminKey, Role.ADMIN, null));
        keys.put(operatorKey, new AuthPrincipal(operatorKey, Role.OPERATOR, null));
        keys.put(googleKey, new AuthPrincipal(googleKey, Role.EMPLOYEE, "Google"));
        keys.put(amazonKey, new AuthPrincipal(amazonKey, Role.EMPLOYEE, "Amazon"));
        this.knownKeys = java.util.Collections.unmodifiableMap(keys);
    }

    /**
     * Authenticates an API key. Rejects missing or unknown keys with SecurityException (HTTP 401).
     */
    public AuthPrincipal authenticate(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new SecurityException("Authentication failed: missing API key");
        }
        AuthPrincipal principal = knownKeys.get(apiKey);
        if (principal == null) {
            throw new SecurityException("Authentication failed: invalid API key");
        }
        return principal;
    }

    /**
     * Enforces strict multi-tenant data boundaries.
     * Uses generic error messaging to avoid leaking tenant identifiers.
     */
    public void enforceTenantAccess(AuthPrincipal principal, String targetClientId) {
        if (principal.role() == Role.ADMIN || principal.role() == Role.OPERATOR) {
            return; // Admins and operators can manage all tenants
        }
        if (principal.assignedTenantId() != null && targetClientId != null) {
            if (!principal.assignedTenantId().equalsIgnoreCase(targetClientId)) {
                throw new SecurityException("Access denied: cross-tenant access is forbidden");
            }
        }
    }
}
