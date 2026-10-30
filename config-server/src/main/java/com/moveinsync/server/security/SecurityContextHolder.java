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

    // Configured API Keys for demo and service verification
    private static final Map<String, AuthPrincipal> KNOWN_KEYS = Map.of(
            "admin-secret-key", new AuthPrincipal("admin-secret-key", Role.ADMIN, null),
            "admin-root-key", new AuthPrincipal("admin-root-key", Role.ADMIN, null),
            "operator-secret-key", new AuthPrincipal("operator-secret-key", Role.OPERATOR, null),
            "google-service-key", new AuthPrincipal("google-service-key", Role.EMPLOYEE, "Google"),
            "amazon-service-key", new AuthPrincipal("amazon-service-key", Role.EMPLOYEE, "Amazon")
    );

    /**
     * Authenticates an API key. Rejects missing or unknown keys with SecurityException (HTTP 401).
     */
    public AuthPrincipal authenticate(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new SecurityException("Authentication failed: missing API key");
        }
        AuthPrincipal principal = KNOWN_KEYS.get(apiKey);
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
