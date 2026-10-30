package com.moveinsync.core;

import com.moveinsync.core.model.ConfigScopeKey;
import com.moveinsync.core.model.Explanation;
import com.moveinsync.core.model.Scope;
import com.moveinsync.core.model.SettingDefinition;
import com.moveinsync.core.model.SettingType;
import com.moveinsync.core.model.ValidationRule;
import com.moveinsync.core.resolution.HierarchyResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class HierarchyResolverTest {

    private HierarchyResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new HierarchyResolver();

        // Register default definition
        resolver.registerDefinition(new SettingDefinition(
                "booking_cutoff_mins",
                SettingType.DURATION_MINUTES,
                60, // Global default
                "Default booking cut-off",
                ValidationRule.durationRange(0, 180)
        ));
    }

    @Test
    @DisplayName("Returns Global default when no overrides exist")
    void shouldReturnGlobalDefault() {
        Optional<Object> value = resolver.resolve("booking_cutoff_mins", "Google", "BLR-1", "NIGHT");
        assertThat(value).contains(60);

        Explanation exp = resolver.explain("booking_cutoff_mins", "Google", "BLR-1", "NIGHT");
        assertThat(exp.effectiveValue()).isEqualTo(60);
        assertThat(exp.resolvedFrom()).isEqualTo(Scope.GLOBAL);
    }

    @Test
    @DisplayName("Client override takes precedence over Global default")
    void clientOverrideWinsOverGlobal() {
        resolver.setOverride(ConfigScopeKey.client("Google"), "booking_cutoff_mins", 45);

        Optional<Object> value = resolver.resolve("booking_cutoff_mins", "Google", "BLR-1", "NIGHT");
        assertThat(value).contains(45);

        Explanation exp = resolver.explain("booking_cutoff_mins", "Google", "BLR-1", "NIGHT");
        assertThat(exp.effectiveValue()).isEqualTo(45);
        assertThat(exp.resolvedFrom()).isEqualTo(Scope.CLIENT);
        assertThat(exp.hierarchy()).containsEntry(Scope.GLOBAL, 60);
        assertThat(exp.hierarchy()).containsEntry(Scope.CLIENT, 45);
    }

    @Test
    @DisplayName("Office override takes precedence over Client and Global")
    void officeOverrideWinsOverClientAndGlobal() {
        resolver.setOverride(ConfigScopeKey.client("Google"), "booking_cutoff_mins", 45);
        resolver.setOverride(ConfigScopeKey.office("Google", "BLR-1"), "booking_cutoff_mins", 30);

        // BLR-1 office gets 30m
        Optional<Object> blrValue = resolver.resolve("booking_cutoff_mins", "Google", "BLR-1", "NIGHT");
        assertThat(blrValue).contains(30);

        // Another office under Google (HYD-1) still gets Client level 45m
        Optional<Object> hydValue = resolver.resolve("booking_cutoff_mins", "Google", "HYD-1", "NIGHT");
        assertThat(hydValue).contains(45);

        // Another client gets Global default 60m
        Optional<Object> amazonValue = resolver.resolve("booking_cutoff_mins", "Amazon", "BLR-1", "NIGHT");
        assertThat(amazonValue).contains(60);
    }

    @Test
    @DisplayName("Shift override is most specific and wins over all tiers")
    void shiftOverrideWinsOverAll() {
        resolver.setOverride(ConfigScopeKey.client("Google"), "booking_cutoff_mins", 45);
        resolver.setOverride(ConfigScopeKey.office("Google", "BLR-1"), "booking_cutoff_mins", 30);
        resolver.setOverride(ConfigScopeKey.shift("Google", "BLR-1", "NIGHT"), "booking_cutoff_mins", 15);

        Optional<Object> nightVal = resolver.resolve("booking_cutoff_mins", "Google", "BLR-1", "NIGHT");
        assertThat(nightVal).contains(15);

        // DAY shift in the same office falls back to Office level (30m)
        Optional<Object> dayVal = resolver.resolve("booking_cutoff_mins", "Google", "BLR-1", "DAY");
        assertThat(dayVal).contains(30);

        Explanation exp = resolver.explain("booking_cutoff_mins", "Google", "BLR-1", "NIGHT");
        assertThat(exp.resolvedFrom()).isEqualTo(Scope.SHIFT);
        assertThat(exp.effectiveValue()).isEqualTo(15);
        assertThat(exp.hierarchy()).containsEntry(Scope.SHIFT, 15);
        assertThat(exp.hierarchy()).containsEntry(Scope.OFFICE, 30);
        assertThat(exp.hierarchy()).containsEntry(Scope.CLIENT, 45);
        assertThat(exp.hierarchy()).containsEntry(Scope.GLOBAL, 60);
    }

    @Test
    @DisplayName("Removing an override cleanly falls back to parent tier")
    void removingOverrideFallsBack() {
        resolver.setOverride(ConfigScopeKey.office("Google", "BLR-1"), "booking_cutoff_mins", 30);
        assertThat(resolver.resolve("booking_cutoff_mins", "Google", "BLR-1", null)).contains(30);

        // Remove office override
        resolver.removeOverride(ConfigScopeKey.office("Google", "BLR-1"), "booking_cutoff_mins");
        assertThat(resolver.resolve("booking_cutoff_mins", "Google", "BLR-1", null)).contains(60); // Back to global
    }
}

