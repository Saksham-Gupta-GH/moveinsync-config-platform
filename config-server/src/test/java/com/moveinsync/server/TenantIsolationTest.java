package com.moveinsync.server;

import com.moveinsync.core.model.ConfigScopeKey;
import com.moveinsync.core.model.Delta;
import com.moveinsync.core.model.SettingDefinition;
import com.moveinsync.core.model.SettingType;
import com.moveinsync.server.controller.ConfigController;
import com.moveinsync.server.security.SecurityContextHolder;
import com.moveinsync.server.service.ConfigService;
import com.moveinsync.server.service.SseBroadcaster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
class TenantIsolationTest {

    @Autowired
    private ConfigController configController;

    @Autowired
    private ConfigService configService;

    @Autowired
    private SseBroadcaster sseBroadcaster;

    @Autowired
    private SecurityContextHolder securityContext;

    @Test
    @DisplayName("Tenant Isolation: Google service key cannot access Amazon configuration")
    void shouldBlockCrossTenantAccess() {
        var principal = securityContext.authenticate("google-service-key");
        assertThat(principal.assignedTenantId()).isEqualTo("Google");

        // Allowed to access Google
        securityContext.enforceTenantAccess(principal, "Google");

        // Blocked from accessing Amazon
        assertThatThrownBy(() -> securityContext.enforceTenantAccess(principal, "Amazon"))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Tenant isolation violation");
    }

    @Test
    @DisplayName("RBAC: Read-only EMPLOYEE role is forbidden from publishing changesets")
    void shouldForbidEmployeeFromPublishing() {
        var req = new ConfigController.PublishChangeSetRequest(
                ConfigScopeKey.client("Google"),
                null,
                Map.of("test_key", 123),
                "employee",
                "Unauthorized edit"
        );

        var response = configController.publishChangeSet("google-service-key", req);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    @DisplayName("SSE Tenant Isolation: Amazon subscriber never receives Google deltas")
    void shouldFilterSseDeltasByTenant() {
        // Subscribe Amazon client
        var amazonEmitter = sseBroadcaster.subscribe("Amazon", "BLR-1");

        // Broadcast a Google delta
        Delta googleDelta = new Delta(99L, ConfigScopeKey.client("Google"), null, Map.of("key", "val"), false, Instant.now());
        sseBroadcaster.broadcast(googleDelta);

        // Find Amazon session in broadcaster
        var amazonSession = sseBroadcaster.getSubscribers().values().stream()
                .filter(s -> "Amazon".equals(s.clientId()))
                .findFirst();

        assertThat(amazonSession).isPresent();
        // Amazon queue must remain empty because delta was scoped to Google
        assertThat(amazonSession.get().queue()).isEmpty();
    }
}
