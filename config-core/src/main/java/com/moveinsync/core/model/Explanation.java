package com.moveinsync.core.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Audit result explaining how a configuration value was resolved across the 4-tier hierarchy.
 */
public record Explanation(
        @JsonProperty("key") String key,
        @JsonProperty("effectiveValue") Object effectiveValue,
        @JsonProperty("resolvedFrom") Scope resolvedFrom,
        @JsonProperty("hierarchy") Map<Scope, Object> hierarchy
) {}
