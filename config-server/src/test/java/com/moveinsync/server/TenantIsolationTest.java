package com.moveinsync.server;

import com.moveinsync.client.cache.ConfigSnapshot;
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
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.util.List;
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
    @DisplayName("Authentication: Missing or unknown API key is rejected with SecurityException")
    void shouldRejectUnauthenticatedRequests() {
        // Missing key
        assertThatThrownBy(() -> securityContext.authenticate(null))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("missing API key");

        // Unknown key
        assertThatThrownBy(() -> securityContext.authenticate("bad-unknown-token"))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("invalid API key");
    }

    @Test
    @DisplayName("Tenant Isolation: Google service key cannot access Amazon configuration")
    void shouldBlockCrossTenantAccess() {
        var principal = securityContext.authenticate("google-service-key");
        assertThat(principal.assignedTenantId()).isEqualTo("Google");

        // Allowed to access Google
        securityContext.enforceTenantAccess(principal, "Google");

        // Blocked from accessing Amazon with generic error message
        assertThatThrownBy(() -> securityContext.enforceTenantAccess(principal, "Amazon"))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Access denied");
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
    @DisplayName("REST Data Isolation: Google snapshot and deltas never contain Amazon data")
    void shouldFilterSnapshotAndDeltasByTenant() {
        // Register test definitions
        configService.registerDefinition(new SettingDefinition("amazon_confidential_rate", SettingType.NUMBER, 100, "Confidential", null));
        configService.registerDefinition(new SettingDefinition("google_confidential_rate", SettingType.NUMBER, 200, "Confidential", null));

        // Publish Amazon changeset as Admin
        configService.publishChangeSet(
                ConfigScopeKey.client("Amazon"),
                null,
                Map.of("amazon_confidential_rate", 999),
                "admin",
                "Amazon rate update"
        );

        // Publish Google changeset as Admin
        configService.publishChangeSet(
                ConfigScopeKey.client("Google"),
                null,
                Map.of("google_confidential_rate", 555),
                "admin",
                "Google rate update"
        );

        // 1. Query Google snapshot via controller
        ResponseEntity<?> snapResp = configController.getSnapshot("google-service-key", "Google", "BLR-1");
        assertThat(snapResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        ConfigSnapshot snap = (ConfigSnapshot) snapResp.getBody();
        assertThat(snap).isNotNull();

        // Must contain Google override but NEVER Amazon override
        assertThat(snap.overrides()).containsKey("CLIENT:Google");
        assertThat(snap.overrides()).doesNotContainKey("CLIENT:Amazon");

        // 2. Query Google deltas via controller
        ResponseEntity<?> deltaResp = configController.getDeltas("google-service-key", "Google", "BLR-1", 0L);
        assertThat(deltaResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        @SuppressWarnings("unchecked")
        List<Delta> deltas = (List<Delta>) deltaResp.getBody();
        assertThat(deltas).isNotNull();

        // Any delta for Amazon must be emptyProgression (empty=true, empty changes)
        for (Delta d : deltas) {
            if ("Amazon".equalsIgnoreCase(d.scopeKey().clientId())) {
                assertThat(d.empty()).isTrue();
                assertThat(d.changes()).isEmpty();
            }
        }
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
