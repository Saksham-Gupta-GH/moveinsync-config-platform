package com.moveinsync.core;

import com.moveinsync.core.model.SettingDefinition;
import com.moveinsync.core.model.SettingType;
import com.moveinsync.core.model.TimeWindow;
import com.moveinsync.core.model.ValidationRule;
import com.moveinsync.core.validator.SettingValidator;
import com.moveinsync.core.validator.ValidationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SettingValidatorTest {

    @Test
    @DisplayName("Boolean settings require JSON Boolean values, not strings")
    void shouldRequireBooleanType() {
        SettingDefinition def = new SettingDefinition(
                "night_escort_required",
                SettingType.BOOLEAN,
                true,
                "Night escort required",
                ValidationRule.none()
        );

        SettingValidator.validate(def, true);
        SettingValidator.validate(def, false);
        for (Object invalid : new Object[] {"true", "false", "hello", 1}) {
            assertThatThrownBy(() -> SettingValidator.validate(def, invalid))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("Expected boolean value");
        }
    }

    @Test
    @DisplayName("Negative duration cut-offs (e.g. -30 min) must be strictly rejected")
    void shouldRejectNegativeDuration() {
        SettingDefinition def = new SettingDefinition(
                "booking_cutoff_mins",
                SettingType.DURATION_MINUTES,
                60,
                "Booking cut-off time",
                ValidationRule.durationRange(0, 720)
        );

        assertThatThrownBy(() -> SettingValidator.validate(def, -30))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("cannot be negative");
    }

    @Test
    @DisplayName("Valid duration within range passes validation")
    void shouldAcceptValidDuration() {
        SettingDefinition def = new SettingDefinition(
                "booking_cutoff_mins",
                SettingType.DURATION_MINUTES,
                60,
                "Booking cut-off time",
                ValidationRule.durationRange(15, 120)
        );

        SettingValidator.validate(def, 45); // Should pass without exception
    }

    @Test
    @DisplayName("Duration above maximum allowed is rejected")
    void shouldRejectDurationAboveMax() {
        SettingDefinition def = new SettingDefinition(
                "booking_cutoff_mins",
                SettingType.DURATION_MINUTES,
                60,
                "Booking cut-off time",
                ValidationRule.durationRange(15, 120)
        );

        assertThatThrownBy(() -> SettingValidator.validate(def, 180))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("exceeds maximum allowed");
    }

    @Test
    @DisplayName("TimeWindow with identical start and end time is rejected")
    void shouldRejectIdenticalTimeWindow() {
        SettingDefinition def = new SettingDefinition(
                "night_safety_window",
                SettingType.TIME_WINDOW,
                new TimeWindow("20:00", "06:00"),
                "Night safety escort window",
                ValidationRule.none()
        );

        assertThatThrownBy(() -> SettingValidator.validate(def, new TimeWindow("20:00", "20:00")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("cannot be identical");
    }

    @Test
    @DisplayName("Invalid time format string is rejected")
    void shouldRejectMalformedTimeWindow() {
        SettingDefinition def = new SettingDefinition(
                "night_safety_window",
                SettingType.TIME_WINDOW,
                new TimeWindow("20:00", "06:00"),
                "Night safety escort window",
                ValidationRule.none()
        );

        Map<String, String> malformed = Map.of("start", "25:99", "end", "06:00");
        assertThatThrownBy(() -> SettingValidator.validate(def, malformed))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Invalid time format");
    }

    @Test
    @DisplayName("Enum value outside allowed whitelist is rejected")
    void shouldRejectDisallowedEnum() {
        SettingDefinition def = new SettingDefinition(
                "allowed_cab_type",
                SettingType.ENUM,
                "SEDAN",
                "Allowed cab type",
                ValidationRule.enumValues(Set.of("SEDAN", "SUV", "HATCHBACK"))
        );

        assertThatThrownBy(() -> SettingValidator.validate(def, "HELICOPTER"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("not permitted");
    }
}

