package com.moveinsync.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Message payload pushed across SSE streams to propagate changes live to subscribers.
 */
public record Delta(
        @JsonProperty("version") long version,
        @JsonProperty("scopeKey") ConfigScopeKey scopeKey,
        @JsonProperty("stagedOffices") List<String> stagedOffices,
        @JsonProperty("changes") Map<String, Object> changes,
        @JsonProperty("empty") boolean empty,
        @JsonProperty("timestamp") Instant timestamp,
        @JsonProperty("replacementOverrides") Map<String, Map<String, Object>> replacementOverrides
) {
    public Delta(long version, ConfigScopeKey scopeKey, List<String> stagedOffices,
                 Map<String, Object> changes, boolean empty, Instant timestamp) {
        this(version, scopeKey, stagedOffices, changes, empty, timestamp, null);
    }

    @JsonCreator
    public Delta {
        Objects.requireNonNull(scopeKey, "scopeKey cannot be null");
        if (changes == null) {
            changes = Collections.emptyMap();
        }
        if (replacementOverrides != null) {
            replacementOverrides = replacementOverrides.entrySet().stream().collect(
                    java.util.stream.Collectors.toUnmodifiableMap(Map.Entry::getKey,
                            entry -> Map.copyOf(entry.getValue())));
        }
        if (stagedOffices == null) {
            stagedOffices = Collections.emptyList();
        }
        if (timestamp == null) timestamp = Instant.now();
    }

    public Delta(long version, ConfigScopeKey scopeKey, Map<String, Object> changes) {
        this(version, scopeKey, Collections.emptyList(), changes, false, Instant.now(), null);
    }

    /**
     * Creates an empty delta representing a contiguous version progression for clients outside a staged canary scope.
     */
    public static Delta emptyProgression(long version, ConfigScopeKey scopeKey) {
        return new Delta(version, scopeKey, Collections.emptyList(), Collections.emptyMap(), true, Instant.now(), null);
    }
}
