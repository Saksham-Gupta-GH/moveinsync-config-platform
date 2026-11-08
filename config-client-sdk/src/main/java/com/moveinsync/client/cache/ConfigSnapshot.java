package com.moveinsync.client.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.moveinsync.core.model.ConfigScopeKey;
import com.moveinsync.core.model.Explanation;
import com.moveinsync.core.model.Scope;
import com.moveinsync.core.model.SettingDefinition;

import java.util.*;

/**
 * An immutable point-in-time snapshot of the tenant configuration.
 * Held in an AtomicReference inside the Client SDK to provide lock-free microsecond reads.
 */
public record ConfigSnapshot(
        @JsonProperty("version") long version,
        @JsonProperty("definitions") Map<String, SettingDefinition> definitions,
        @JsonProperty("overrides") Map<String, Map<String, Object>> overrides
) {
    @JsonCreator
    public ConfigSnapshot {
        definitions = definitions != null ? Map.copyOf(definitions) : Map.of();
        if (overrides != null) {
            Map<String, Map<String, Object>> deepCopy = new HashMap<>();
            overrides.forEach((k, v) -> deepCopy.put(k, Map.copyOf(v)));
            overrides = Collections.unmodifiableMap(deepCopy);
        } else {
            overrides = Map.of();
        }
    }

    public static ConfigSnapshot empty() {
        return new ConfigSnapshot(0L, Map.of(), Map.of());
    }

    /**
     * Resolves the setting value across the 4-tier hierarchy.
     */
    public Optional<Object> resolve(String key, String clientId, String officeId, String shiftId) {
        // 1. Shift
        if (clientId != null && officeId != null && shiftId != null) {
            Object v = getValue(ConfigScopeKey.shift(clientId, officeId, shiftId).toPath(), key);
            if (v != null) return Optional.of(v);
        }

        // 2. Office
        if (clientId != null && officeId != null) {
            Object v = getValue(ConfigScopeKey.office(clientId, officeId).toPath(), key);
            if (v != null) return Optional.of(v);
        }

        // 3. Client
        if (clientId != null) {
            Object v = getValue(ConfigScopeKey.client(clientId).toPath(), key);
            if (v != null) return Optional.of(v);
        }

        // 4. Global override
        Object g = getValue(ConfigScopeKey.global().toPath(), key);
        if (g != null) return Optional.of(g);

        // 5. Global Default
        SettingDefinition def = definitions.get(key);
        if (def != null && def.defaultValue() != null) {
            return Optional.of(def.defaultValue());
        }

        return Optional.empty();
    }

    public Explanation explain(String key, String clientId, String officeId, String shiftId) {
        Map<Scope, Object> hierarchy = new EnumMap<>(Scope.class);

        if (clientId != null && officeId != null && shiftId != null) {
            Object v = getValue(ConfigScopeKey.shift(clientId, officeId, shiftId).toPath(), key);
            if (v != null) hierarchy.put(Scope.SHIFT, v);
        }
        if (clientId != null && officeId != null) {
            Object v = getValue(ConfigScopeKey.office(clientId, officeId).toPath(), key);
            if (v != null) hierarchy.put(Scope.OFFICE, v);
        }
        if (clientId != null) {
            Object v = getValue(ConfigScopeKey.client(clientId).toPath(), key);
            if (v != null) hierarchy.put(Scope.CLIENT, v);
        }
        Object g = getValue(ConfigScopeKey.global().toPath(), key);
        if (g != null) {
            hierarchy.put(Scope.GLOBAL, g);
        }
        SettingDefinition def = definitions.get(key);
        if (def != null && def.defaultValue() != null) {
            hierarchy.put(Scope.DEFAULT, def.defaultValue());
        }

        if (hierarchy.containsKey(Scope.SHIFT)) return new Explanation(key, hierarchy.get(Scope.SHIFT), Scope.SHIFT, hierarchy);
        if (hierarchy.containsKey(Scope.OFFICE)) return new Explanation(key, hierarchy.get(Scope.OFFICE), Scope.OFFICE, hierarchy);
        if (hierarchy.containsKey(Scope.CLIENT)) return new Explanation(key, hierarchy.get(Scope.CLIENT), Scope.CLIENT, hierarchy);
        if (hierarchy.containsKey(Scope.GLOBAL)) return new Explanation(key, hierarchy.get(Scope.GLOBAL), Scope.GLOBAL, hierarchy);
        if (hierarchy.containsKey(Scope.DEFAULT)) return new Explanation(key, hierarchy.get(Scope.DEFAULT), Scope.DEFAULT, hierarchy);

        return new Explanation(key, null, null, hierarchy);
    }

    private Object getValue(String scopePath, String key) {
        Map<String, Object> map = overrides.get(scopePath);
        return map != null ? map.get(key) : null;
    }

    /**
     * Creates an updated snapshot advancing the version with an applied delta.
     */
    public ConfigSnapshot applyDelta(long newVersion, ConfigScopeKey scopeKey, Map<String, Object> changes) {
        Map<String, Map<String, Object>> newOverrides = new HashMap<>();
        overrides.forEach((k, v) -> newOverrides.put(k, new HashMap<>(v)));

        if (scopeKey != null && changes != null && !changes.isEmpty()) {
            Map<String, Object> targetScopeMap = newOverrides.computeIfAbsent(scopeKey.toPath(), k -> new HashMap<>());
            targetScopeMap.putAll(changes);
        }

        return new ConfigSnapshot(newVersion, definitions, newOverrides);
    }

    public ConfigSnapshot withVersion(long newVersion) {
        return new ConfigSnapshot(newVersion, definitions, overrides);
    }

    public ConfigSnapshot withDefinitions(Map<String, SettingDefinition> newDefs) {
        Map<String, SettingDefinition> merged = new HashMap<>(definitions);
        merged.putAll(newDefs);
        return new ConfigSnapshot(version, merged, overrides);
    }
}

