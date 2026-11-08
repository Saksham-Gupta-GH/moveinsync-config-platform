package com.moveinsync.server;

import com.moveinsync.core.model.ConfigScopeKey;
import com.moveinsync.core.model.SettingDefinition;
import com.moveinsync.core.model.SettingType;
import com.moveinsync.core.model.ValidationRule;
import com.moveinsync.core.validator.ValidationException;
import com.moveinsync.server.service.ConfigService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
class CompoundValidationRejectionTest {

    @Autowired
    private ConfigService configService;

    @Test
    @DisplayName("Atomic Validation: If 1 key in a compound changeset is invalid (-30 min), nothing is published and version does not advance")
    void shouldRejectEntireChangesetIfOneKeyIsInvalid() {
        configService.registerDefinition(new SettingDefinition(
                "valid_key", SettingType.NUMBER, 10, "Valid", ValidationRule.none()));
        configService.registerDefinition(new SettingDefinition(
                "invalid_cutoff", SettingType.DURATION_MINUTES, 60, "Cutoff", ValidationRule.durationRange(0, 180)));

        long versionBefore = configService.getLatestVersion();

        // Compound update: 1 good key, 1 bad key (-30)
        Map<String, Object> compoundUpdates = Map.of(
                "valid_key", 999,
                "invalid_cutoff", -30 // Strictly illegal!
        );

        assertThatThrownBy(() -> configService.publishChangeSet(
                ConfigScopeKey.client("Google"), null, compoundUpdates, "tester", "Compound test"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("cannot be negative");

        // PROVEN: Version did not advance
        assertThat(configService.getLatestVersion()).isEqualTo(versionBefore);

        // PROVEN: valid_key was NOT updated either (atomic all-or-nothing)
        var snapshot = configService.getSnapshot("Google", "BLR-1");
        assertThat(snapshot.resolve("valid_key", "Google", "BLR-1", null)).contains(10);
    }

    @Test
    @DisplayName("An invalid definition default is rejected before it becomes readable")
    void shouldRejectInvalidDefinitionDefault() {
        String key = "invalid_default_cutoff";
        var invalid = new SettingDefinition(key, SettingType.DURATION_MINUTES, -30,
                "Invalid default", ValidationRule.durationRange(0, 180));
        assertThatThrownBy(() -> configService.registerDefinition(invalid))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("cannot be negative");
        assertThat(configService.getDefinitions()).doesNotContainKey(key);
    }
}

