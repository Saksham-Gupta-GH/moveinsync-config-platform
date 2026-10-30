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
}

