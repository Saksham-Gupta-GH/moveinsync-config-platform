package com.moveinsync.client;

import com.moveinsync.client.cache.ConfigSnapshot;
import com.moveinsync.client.persistence.DiskCacheManager;
import com.moveinsync.core.model.ConfigScopeKey;
import com.moveinsync.core.model.Delta;
import com.moveinsync.core.model.SettingDefinition;
import com.moveinsync.core.model.SettingType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigClientTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Delta handles contiguous version progression and updates in-memory snapshot")
    void shouldHandleContiguousDelta() {
        try (MoveInSyncConfigClient client = new MoveInSyncConfigClient(
                "http://localhost:8080", "Google", "BLR-1", "test-key", tempDir)) {

            assertThat(client.getCurrentVersion()).isEqualTo(0L);

            // Apply V1 delta
            Delta d1 = new Delta(1L, ConfigScopeKey.client("Google"), null, Map.of("booking_cutoff_mins", 45), false, null);
            client.handleDelta(d1);

            assertThat(client.getCurrentVersion()).isEqualTo(1L);
            assertThat(client.get("booking_cutoff_mins")).contains(45);
            assertThat(client.get(client.getSnapshot(), "booking_cutoff_mins", Integer.class)).isEqualTo(45);
            org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                    client.get(client.getSnapshot(), "booking_cutoff_mins", String.class))
                    .isInstanceOf(ClassCastException.class);

            // Stale / Duplicate V1 delta should be dropped as a no-op
            client.handleDelta(d1);
            assertThat(client.getCurrentVersion()).isEqualTo(1L);

            // Apply V2 empty delta (e.g. canary staged change for another office)
            Delta d2 = Delta.emptyProgression(2L, ConfigScopeKey.office("Google", "HYD-1"));
            client.handleDelta(d2);

            assertThat(client.getCurrentVersion()).isEqualTo(2L);
            // Original setting remains unchanged
            assertThat(client.get("booking_cutoff_mins")).contains(45);
        }
    }

    @Test
    @DisplayName("DiskCacheManager saves and hydrates snapshot on cold boot")
    void shouldPersistAndLoadFromDisk() {
        DiskCacheManager diskManager = new DiskCacheManager(tempDir);

        ConfigSnapshot snapshot = new ConfigSnapshot(
                42L,
                Map.of("escort_required", new SettingDefinition("escort_required", SettingType.BOOLEAN, false, "Escort", null)),
                Map.of("CLIENT:Google", Map.of("escort_required", true))
        );

        diskManager.saveSnapshot(snapshot);

        Optional<ConfigSnapshot> loaded = diskManager.loadSnapshot();
        assertThat(loaded).isPresent();
        assertThat(loaded.get().version()).isEqualTo(42L);
        assertThat(loaded.get().resolve("escort_required", "Google", "BLR-1", null)).contains(true);
    }

    @Test
    @DisplayName("Rollback replacement removes obsolete overrides in one client snapshot")
    void shouldApplyRollbackReplacementAtomically() throws Exception {
        try (MoveInSyncConfigClient client = new MoveInSyncConfigClient(
                "http://localhost:8080", "Google", "BLR-1", "test-key", tempDir)) {
            client.handleDelta(new Delta(1L, ConfigScopeKey.client("Google"),
                    Map.of("cutoff", 60)));
            client.handleDelta(new Delta(2L, ConfigScopeKey.office("Google", "BLR-1"),
                    Map.of("cutoff", 30)));
            assertThat(client.get("cutoff")).contains(30);

            Delta rollback = new Delta(3L, ConfigScopeKey.global(), java.util.List.of(),
                    Map.of(), false, null, Map.of("CLIENT:Google", Map.of("cutoff", 60)));
            var mapper = new com.fasterxml.jackson.databind.ObjectMapper()
                    .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
            client.handleDelta(mapper.readValue(mapper.writeValueAsString(rollback), Delta.class));
            assertThat(client.getCurrentVersion()).isEqualTo(3L);
            assertThat(client.get("cutoff")).contains(60);
            assertThat(client.getSnapshot().overrides()).doesNotContainKey("OFFICE:Google/BLR-1");
        }
    }
}

