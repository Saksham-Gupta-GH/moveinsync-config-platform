package com.moveinsync.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An atomic unit of mutation containing one or more setting updates committed under a single monotonic version.
 */
public record ChangeSet(
        @JsonProperty("id") String id,
        @JsonProperty("version") long version,
        @JsonProperty("scopeKey") ConfigScopeKey scopeKey,
        @JsonProperty("stagedOffices") List<String> stagedOffices,
        @JsonProperty("updates") Map<String, Object> updates,
        @JsonProperty("author") String author,
        @JsonProperty("reason") String reason,
        @JsonProperty("timestamp") Instant timestamp,
        @JsonProperty("replacementOverrides") Map<String, Map<String, Object>> replacementOverrides
) {
    public ChangeSet(String id, long version, ConfigScopeKey scopeKey, List<String> stagedOffices,
                     Map<String, Object> updates, String author, String reason, Instant timestamp) {
        this(id, version, scopeKey, stagedOffices, updates, author, reason, timestamp, null);
    }

    @JsonCreator
    public ChangeSet {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(scopeKey, "scopeKey cannot be null");
        Objects.requireNonNull(updates, "updates cannot be null");
        if (updates.isEmpty() && replacementOverrides == null) {
            throw new IllegalArgumentException("updates map cannot be empty");
        }
        if (replacementOverrides != null) {
            replacementOverrides = replacementOverrides.entrySet().stream().collect(
                    java.util.stream.Collectors.toUnmodifiableMap(Map.Entry::getKey,
                            entry -> Map.copyOf(entry.getValue())));
        }
        if (stagedOffices == null) {
            stagedOffices = Collections.emptyList();
        }
        if (author == null) author = "system";
        if (reason == null) reason = "configuration update";
        if (timestamp == null) timestamp = Instant.now();
    }
}

