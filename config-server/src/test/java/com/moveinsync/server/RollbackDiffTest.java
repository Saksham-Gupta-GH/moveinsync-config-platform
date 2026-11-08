package com.moveinsync.server;

import com.moveinsync.core.model.ConfigScopeKey;
import com.moveinsync.core.model.SettingDefinition;
import com.moveinsync.core.model.SettingType;
import com.moveinsync.core.model.ValidationRule;
import com.moveinsync.server.service.ConfigService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class RollbackDiffTest {

    @Autowired
    private ConfigService configService;

    @Test
    @DisplayName("Rollback: Dry-run preview displays before-vs-after diff; execution applies rollback as new forward version")
    void shouldPreviewDiffAndRollback() {
        configService.registerDefinition(new SettingDefinition(
                "max_ride_time_mins", SettingType.NUMBER, 90, "Max ride time", ValidationRule.numberRange(30, 180)));

        long v0 = configService.getLatestVersion();

        // V1: set to 100
        configService.publishChangeSet(ConfigScopeKey.client("Google"), null, Map.of("max_ride_time_mins", 100), "admin", "Increase limit");
        long v1 = configService.getLatestVersion();

        // V2: set to 120
        configService.publishChangeSet(ConfigScopeKey.client("Google"), null, Map.of("max_ride_time_mins", 120), "admin", "Increase again");
        long v2 = configService.getLatestVersion();

        // 1. Dry-run rollback to V1
        var dryRunResp = configService.rollback(v1, true, "admin");
        assertThat(dryRunResp.dryRun()).isTrue();
        assertThat(dryRunResp.currentVersion()).isEqualTo(v2);
        assertThat(dryRunResp.targetVersion()).isEqualTo(v1);
        assertThat(dryRunResp.diff()).containsKey("CLIENT:Google");
        assertThat(dryRunResp.diff().get("CLIENT:Google")).containsEntry("max_ride_time_mins", 100);
        // Version should NOT have changed on dry run
        assertThat(configService.getLatestVersion()).isEqualTo(v2);

        // 2. Commit Rollback to V1
        var commitResp = configService.rollback(v1, false, "admin");
        assertThat(commitResp.dryRun()).isFalse();
        long v3 = configService.getLatestVersion();
        assertThat(v3).isEqualTo(v2 + 1);

        // Effective value is now back to 100
        var snapshot = configService.getSnapshot("Google", "BLR-1");
        assertThat(snapshot.resolve("max_ride_time_mins", "Google", "BLR-1", null)).contains(100);
    }

    @Test
    @DisplayName("Rollback removes later overrides across scopes in one version and restores inheritance")
    void shouldRollbackMultipleScopesAtomically() {
        String key = "rollback_inheritance_" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        configService.registerDefinition(new SettingDefinition(
                key, SettingType.NUMBER, 60, "Rollback inheritance", ValidationRule.none()));
        configService.publishChangeSet(ConfigScopeKey.client("Google"), null,
                Map.of(key, 70), "admin", "Baseline client value");
        long targetVersion = configService.getLatestVersion();

        configService.publishChangeSet(ConfigScopeKey.office("Google", "BLR-1"), null,
                Map.of(key, 30), "admin", "Later office override");
        configService.publishChangeSet(ConfigScopeKey.client("Amazon"), null,
                Map.of(key, 55), "admin", "Later tenant override");
        long beforeRollback = configService.getLatestVersion();

        var preview = configService.rollback(targetVersion, true, "admin");
        assertThat(preview.diff().get("OFFICE:Google/BLR-1")).containsEntry(key, null);
        assertThat(preview.diff().get("CLIENT:Amazon")).containsEntry(key, null);
        assertThat(configService.getLatestVersion()).isEqualTo(beforeRollback);

        var result = configService.rollback(targetVersion, false, "admin");
        assertThat(result.committedVersion()).isEqualTo(beforeRollback + 1);
        assertThat(configService.getLatestVersion()).isEqualTo(beforeRollback + 1);
        assertThat(configService.getSnapshot("Google", "BLR-1")
                .resolve(key, "Google", "BLR-1", null)).contains(70);
        assertThat(configService.getSnapshot("Amazon", "BLR-1")
                .resolve(key, "Amazon", "BLR-1", null)).contains(60);
        assertThat(configService.getSnapshot("Google", "BLR-1").overrides()
                .getOrDefault("OFFICE:Google/BLR-1", Map.of())).doesNotContainKey(key);

        var googleDelta = configService.getDeltas("Google", "BLR-1", beforeRollback).get(0);
        assertThat(googleDelta.replacementOverrides()).containsKey("CLIENT:Google");
        assertThat(googleDelta.replacementOverrides()).doesNotContainKey("CLIENT:Amazon");
    }

    @Test
    @DisplayName("Historical rollback replays canary promotion and earlier rollback events")
    void shouldRestoreVersionsAroundCanaryPromotion() {
        String key = "rollback_canary_" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        configService.registerDefinition(new SettingDefinition(
                key, SettingType.BOOLEAN, false, "Rollback canary history", ValidationRule.none()));
        var canary = configService.publishChangeSet(ConfigScopeKey.client("Google"),
                List.of("BLR-1"), Map.of(key, true), "admin", "Stage canary");
        var promoted = configService.promoteChangeSet(canary.id(), "admin");

        configService.rollback(canary.version(), false, "admin");
        assertThat(configService.getSnapshot("Google", "BLR-1")
                .resolve(key, "Google", "BLR-1", null)).contains(true);
        assertThat(configService.getSnapshot("Google", "HYD-1")
                .resolve(key, "Google", "HYD-1", null)).contains(false);

        configService.rollback(promoted.version(), false, "admin");
        assertThat(configService.getSnapshot("Google", "BLR-1")
                .resolve(key, "Google", "BLR-1", null)).contains(true);
        assertThat(configService.getSnapshot("Google", "HYD-1")
                .resolve(key, "Google", "HYD-1", null)).contains(true);
    }
}

