package com.moveinsync.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Metadata definition for a configuration key.
 */
public record SettingDefinition(
        @JsonProperty("key") String key,
        @JsonProperty("type") SettingType type,
        @JsonProperty("defaultValue") Object defaultValue,
        @JsonProperty("description") String description,
        @JsonProperty("validationRule") ValidationRule validationRule
) {
    @JsonCreator
    public SettingDefinition {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(type, "type cannot be null");
        Objects.requireNonNull(defaultValue, "defaultValue cannot be null");
        if (validationRule == null) {
            validationRule = ValidationRule.none();
        }
    }
}
