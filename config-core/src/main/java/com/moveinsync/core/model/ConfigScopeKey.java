package com.moveinsync.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Composite key representing the specific target tier for an override.
 */
public record ConfigScopeKey(
        @JsonProperty("scope") Scope scope,
        @JsonProperty("clientId") String clientId,
        @JsonProperty("officeId") String officeId,
        @JsonProperty("shiftId") String shiftId
) {
    @JsonCreator
    public ConfigScopeKey {
        Objects.requireNonNull(scope, "scope cannot be null");
        if (scope == Scope.DEFAULT) {
            throw new IllegalArgumentException("DEFAULT is a fallback, not an override scope");
        }
    }

    public static ConfigScopeKey global() {
        return new ConfigScopeKey(Scope.GLOBAL, null, null, null);
    }

    public static ConfigScopeKey client(String clientId) {
        Objects.requireNonNull(clientId, "clientId cannot be null for CLIENT scope");
        return new ConfigScopeKey(Scope.CLIENT, clientId, null, null);
    }

    public static ConfigScopeKey office(String clientId, String officeId) {
        Objects.requireNonNull(clientId, "clientId cannot be null for OFFICE scope");
        Objects.requireNonNull(officeId, "officeId cannot be null for OFFICE scope");
        return new ConfigScopeKey(Scope.OFFICE, clientId, officeId, null);
    }

    public static ConfigScopeKey shift(String clientId, String officeId, String shiftId) {
        Objects.requireNonNull(clientId, "clientId cannot be null for SHIFT scope");
        Objects.requireNonNull(officeId, "officeId cannot be null for SHIFT scope");
        Objects.requireNonNull(shiftId, "shiftId cannot be null for SHIFT scope");
        return new ConfigScopeKey(Scope.SHIFT, clientId, officeId, shiftId);
    }

    /**
     * Unique serialized string representation for map keys.
     */
    public String toPath() {
        return switch (scope) {
            case DEFAULT -> throw new IllegalStateException("DEFAULT is not an override scope");
            case GLOBAL -> "GLOBAL";
            case CLIENT -> "CLIENT:" + clientId;
            case OFFICE -> "OFFICE:" + clientId + "/" + officeId;
            case SHIFT -> "SHIFT:" + clientId + "/" + officeId + "/" + shiftId;
        };
    }
}

