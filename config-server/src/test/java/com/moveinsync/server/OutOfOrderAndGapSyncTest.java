package com.moveinsync.server;

import com.moveinsync.client.MoveInSyncConfigClient;
import com.moveinsync.core.model.*;
import com.moveinsync.server.service.ConfigService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OutOfOrderAndGapSyncTest {

    @LocalServerPort
    private int port;

    @Autowired
    private ConfigService configService;

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Out-of-Order & Gap Sync: Detecting non-contiguous delta triggers automatic REST catch-up and drops stale duplicates")
    void shouldDetectGapAndHealViaCatchup() throws Exception {
        configService.registerDefinition(new SettingDefinition(
                "escort_cutoff", SettingType.NUMBER, 15, "Cutoff", ValidationRule.none()));

        String serverUrl = "http://localhost:" + port;

        try (MoveInSyncConfigClient client = new MoveInSyncConfigClient(
                serverUrl, "Google", "BLR-1", "google-service-key", tempDir)) {
            client.start();

            // Establish base version
            long baseVer = client.getCurrentVersion();

            // Server produces V(base+1) and V(base+2)
            ChangeSet cs1 = configService.publishChangeSet(
                    ConfigScopeKey.client("Google"), null, Map.of("escort_cutoff", 20), "admin", "Step 1");
            long v1 = cs1.version();

            ChangeSet cs2 = configService.publishChangeSet(
                    ConfigScopeKey.client("Google"), null, Map.of("escort_cutoff", 30), "admin", "Step 2");
            long v2 = cs2.version();

            // Simulate Network reordering / gap:
            // Feed client directly with V2 (skipping V1)
            Delta gapDelta = new Delta(v2, ConfigScopeKey.client("Google"), Map.of("escort_cutoff", 30));
            client.handleDelta(gapDelta);

            // Wait brief moment for async fetchMissingDeltas() or synchronous healing
            Thread.sleep(300);

            // Client should have automatically fetched missing delta from server and caught up to V2!
            assertThat(client.getCurrentVersion()).isEqualTo(v2);
            assertThat(client.get("escort_cutoff")).contains(30);

            // Stale / Duplicate test: Deliver V1 again now that client is at V2
            Delta staleDelta = new Delta(v1, ConfigScopeKey.client("Google"), Map.of("escort_cutoff", 999));
            client.handleDelta(staleDelta);

            // Client version and value remain unchanged (stale ignored!)
            assertThat(client.getCurrentVersion()).isEqualTo(v2);
            assertThat(client.get("escort_cutoff")).contains(30);

            // Contiguous progression test: deliver V(v2+1)
            ChangeSet cs3 = configService.publishChangeSet(
                    ConfigScopeKey.client("Google"), null, Map.of("escort_cutoff", 45), "admin", "Step 3");
            long v3 = cs3.version();

            Delta contiguousDelta = new Delta(v3, ConfigScopeKey.client("Google"), Map.of("escort_cutoff", 45));
            client.handleDelta(contiguousDelta);

            assertThat(client.getCurrentVersion()).isEqualTo(v3);
            assertThat(client.get("escort_cutoff")).contains(45);
        }
    }
}
