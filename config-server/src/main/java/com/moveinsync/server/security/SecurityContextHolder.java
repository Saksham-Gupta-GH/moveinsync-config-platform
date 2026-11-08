package com.moveinsync.server.security;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import java.security.SecureRandom;
import java.time.Instant;

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
    private final Map<String, BrowserSession> browserSessions = new ConcurrentHashMap<>();
    private final SecureRandom secureRandom = new SecureRandom();
    private static final long SESSION_SECONDS = 2 * 60 * 60;

    private record BrowserSession(AuthPrincipal principal, Instant expiresAt) {}

    public SecurityContextHolder() {
        // Demo keys require an explicit local opt-in. Production always requires fresh secrets.
        boolean production = java.util.Arrays.asList(System.getenv().getOrDefault("SPRING_PROFILES_ACTIVE", "").split(","))
                .contains("prod");
        boolean localDemo = !production && Boolean.parseBoolean(System.getenv("MOVEINSYNC_ALLOW_DEMO_KEYS"));
        if (!localDemo) {
            for (String name : new String[]{"MOVEINSYNC_ADMIN_KEY", "MOVEINSYNC_OPERATOR_KEY",
                    "MOVEINSYNC_GOOGLE_KEY", "MOVEINSYNC_AMAZON_KEY"}) {
                String value = System.getenv(name);
                if (value == null || value.length() < 24 || value.equals("admin-secret-key")
                        || value.equals("operator-secret-key") || value.equals("google-service-key")
                        || value.equals("amazon-service-key")) {
                    throw new IllegalStateException(name + " must be set to a unique secret of at least 24 characters in production");
                }
            }
        }
        String adminKey = System.getenv().getOrDefault("MOVEINSYNC_ADMIN_KEY", "admin-secret-key");
        String operatorKey = System.getenv().getOrDefault("MOVEINSYNC_OPERATOR_KEY", "operator-secret-key");
        String googleKey = System.getenv().getOrDefault("MOVEINSYNC_GOOGLE_KEY", "google-service-key");
        String amazonKey = System.getenv().getOrDefault("MOVEINSYNC_AMAZON_KEY", "amazon-service-key");

        Map<String, AuthPrincipal> keys = new java.util.HashMap<>();
        keys.put(adminKey, new AuthPrincipal(adminKey, Role.ADMIN, null));
        keys.put(operatorKey, new AuthPrincipal(operatorKey, Role.OPERATOR, null));
        keys.put(googleKey, new AuthPrincipal(googleKey, Role.EMPLOYEE, "Google"));
        keys.put(amazonKey, new AuthPrincipal(amazonKey, Role.EMPLOYEE, "Amazon"));
        if (keys.size() != 4) throw new IllegalStateException("API keys must be unique");
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

    public String createBrowserSession(String apiKey) {
        AuthPrincipal principal = authenticate(apiKey);
        if (principal.role() != Role.ADMIN && principal.role() != Role.OPERATOR) {
            throw new SecurityException("Access denied: dashboard requires an administrator or operator key");
        }
        browserSessions.entrySet().removeIf(entry -> entry.getValue().expiresAt().isBefore(Instant.now()));
        byte[] random = new byte[32];
        secureRandom.nextBytes(random);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(random);
        browserSessions.put(token, new BrowserSession(principal, Instant.now().plusSeconds(SESSION_SECONDS)));
        return token;
    }

    public String browserSessionApiKey(String token) {
        if (token == null) return null;
        BrowserSession session = browserSessions.get(token);
        if (session == null) return null;
        if (!session.expiresAt().isAfter(Instant.now())) {
            browserSessions.remove(token);
            return null;
        }
        return session.principal().apiKey();
    }

    public void revokeBrowserSession(String token) {
        if (token != null) browserSessions.remove(token);
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
