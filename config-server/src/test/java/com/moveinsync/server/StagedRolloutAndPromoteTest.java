package com.moveinsync.server;

import com.moveinsync.core.model.ChangeSet;
import com.moveinsync.core.model.ConfigScopeKey;
import com.moveinsync.core.model.Delta;
import com.moveinsync.core.model.SettingDefinition;
import com.moveinsync.core.model.SettingType;
import com.moveinsync.core.model.ValidationRule;
import com.moveinsync.server.service.ConfigService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class StagedRolloutAndPromoteTest {

    @Autowired
    private ConfigService configService;

    @Test
    @DisplayName("Staged Canary Rollout: Canary offices receive update, out-of-scope offices receive contiguous empty delta, promote expands to all")
    void shouldHandleStagedCanaryAndPromotion() {
        configService.registerDefinition(new SettingDefinition(
                "staged_feature_flag", SettingType.BOOLEAN, false, "Feature flag", ValidationRule.none()));

        long initialVer = configService.getLatestVersion();

        // 1. Stage canary rollout for BLR-1 only
        ChangeSet canary = configService.publishChangeSet(
                ConfigScopeKey.client("Google"),
                List.of("BLR-1"),
                Map.of("staged_feature_flag", true),
                "admin",
                "Canary test in BLR-1"
        );

        long canaryVer = canary.version();
        assertThat(canaryVer).isEqualTo(initialVer + 1);

        // 2. Verify delta query: BLR-1 receives full delta, HYD-1 receives contiguous empty delta
        List<Delta> blrDeltas = configService.getDeltas("Google", "BLR-1", initialVer);
        assertThat(blrDeltas).hasSize(1);
        assertThat(blrDeltas.get(0).empty()).isFalse();
        assertThat(blrDeltas.get(0).changes()).containsEntry("staged_feature_flag", true);

        List<Delta> hydDeltas = configService.getDeltas("Google", "HYD-1", initialVer);
        assertThat(hydDeltas).hasSize(1);
        // EVALUATOR FIX PROVEN: HYD-1 receives version progression without false gap!
        assertThat(hydDeltas.get(0).version()).isEqualTo(canaryVer);
        assertThat(hydDeltas.get(0).empty()).isTrue();

        // 2b. Verify snapshot resolution isolation: BLR-1 resolves canary (true), HYD-1 resolves default (false)
        com.moveinsync.client.cache.ConfigSnapshot snapshot = configService.getSnapshot("Google", "ALL");
        assertThat(snapshot.resolve("staged_feature_flag", "Google", "BLR-1", null).orElse(null)).isEqualTo(true);
        assertThat(snapshot.resolve("staged_feature_flag", "Google", "HYD-1", null).orElse(null)).isEqualTo(false);

        // 3. Promote to all offices under client
        ChangeSet promoted = configService.promoteChangeSet(canary.id(), "admin");
        long promotedVer = promoted.version();
        assertThat(promotedVer).isEqualTo(canaryVer + 1);
        assertThat(promoted.stagedOffices()).isEmpty(); // Global to Google client

        // Now HYD-1 receives the promoted value
        List<Delta> hydPromoted = configService.getDeltas("Google", "HYD-1", canaryVer);
        assertThat(hydPromoted).hasSize(1);
        assertThat(hydPromoted.get(0).empty()).isFalse();
        assertThat(hydPromoted.get(0).changes()).containsEntry("staged_feature_flag", true);

        // 4. Regression check: after promotion, BLR-1 must not have an orphaned OFFICE override shadowing subsequent client updates!
        configService.publishChangeSet(
                ConfigScopeKey.client("Google"),
                List.of(),
                Map.of("staged_feature_flag", false),
                "admin",
                "Subsequent client-wide update to false"
        );

        com.moveinsync.client.cache.ConfigSnapshot postPromoteSnapshot = configService.getSnapshot("Google", "ALL");
        // Prior to the promote fix, BLR-1 remained shadowed with 'true'. Now it resolves 'false' from CLIENT scope!
        assertThat(postPromoteSnapshot.resolve("staged_feature_flag", "Google", "BLR-1", null).orElse(null))
                .isEqualTo(false);
        assertThat(postPromoteSnapshot.resolve("staged_feature_flag", "Google", "HYD-1", null).orElse(null))
                .isEqualTo(false);
    }

    @Test
    @DisplayName("A canary targets two chosen offices and excludes other offices")
    void shouldStageToTwoOffices() {
        String key = "two_office_canary_flag";
        configService.registerDefinition(new SettingDefinition(
                key, SettingType.BOOLEAN, false, "Two office canary", ValidationRule.none()));
        long before = configService.getLatestVersion();
        configService.publishChangeSet(ConfigScopeKey.client("Google"),
                List.of("BLR-1", "PUNE-1"), Map.of(key, true), "admin", "Two office canary");

        for (String office : List.of("BLR-1", "PUNE-1")) {
            assertThat(configService.getSnapshot("Google", office)
                    .resolve(key, "Google", office, null)).contains(true);
            assertThat(configService.getDeltas("Google", office, before).get(0).empty()).isFalse();
        }
        assertThat(configService.getSnapshot("Google", "HYD-1")
                .resolve(key, "Google", "HYD-1", null)).contains(false);
        assertThat(configService.getDeltas("Google", "HYD-1", before).get(0).empty()).isTrue();
    }
}

