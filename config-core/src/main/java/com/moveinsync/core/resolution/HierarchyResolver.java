package com.moveinsync.core.resolution;

import com.moveinsync.core.model.ConfigScopeKey;
import com.moveinsync.core.model.Explanation;
import com.moveinsync.core.model.Scope;
import com.moveinsync.core.model.SettingDefinition;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 4-Tier Hierarchical Configuration Resolution Engine.
 * Resolution precedence: SHIFT -> OFFICE -> CLIENT -> GLOBAL -> Default
 */
public class HierarchyResolver {

    // Global definitions: key -> SettingDefinition
    private final Map<String, SettingDefinition> definitions = new ConcurrentHashMap<>();

    // Overrides: scopePath -> (key -> value)
    private final Map<String, Map<String, Object>> overrides = new ConcurrentHashMap<>();

    public void registerDefinition(SettingDefinition definition) {
        definitions.put(definition.key(), definition);
    }

    public Optional<SettingDefinition> getDefinition(String key) {
        return Optional.ofNullable(definitions.get(key));
    }

    public void setOverride(ConfigScopeKey scopeKey, String key, Object value) {
        overrides.computeIfAbsent(scopeKey.toPath(), k -> new ConcurrentHashMap<>()).put(key, value);
    }

    public void removeOverride(ConfigScopeKey scopeKey, String key) {
        Map<String, Object> map = overrides.get(scopeKey.toPath());
        if (map != null) {
            map.remove(key);
        }
    }

    public void clear() {
        definitions.clear();
        overrides.clear();
    }

    /**
     * Resolves the effective configuration value across the 4-tier hierarchy.
     */
    public Optional<Object> resolve(String key, String clientId, String officeId, String shiftId) {
        // 1. Shift level
        if (clientId != null && officeId != null && shiftId != null) {
            Object val = getValueAtScope(ConfigScopeKey.shift(clientId, officeId, shiftId), key);
            if (val != null) return Optional.of(val);
        }

        // 2. Office level
        if (clientId != null && officeId != null) {
            Object val = getValueAtScope(ConfigScopeKey.office(clientId, officeId), key);
            if (val != null) return Optional.of(val);
        }

        // 3. Client level
        if (clientId != null) {
            Object val = getValueAtScope(ConfigScopeKey.client(clientId), key);
            if (val != null) return Optional.of(val);
        }

        // 4. Global level override
        Object globalVal = getValueAtScope(ConfigScopeKey.global(), key);
        if (globalVal != null) return Optional.of(globalVal);

        // 5. Global Default from Setting Definition
        SettingDefinition def = definitions.get(key);
        if (def != null && def.defaultValue() != null) {
            return Optional.of(def.defaultValue());
        }

        return Optional.empty();
    }

    /**
     * Explains which tier supplied the effective value and lists all parent overrides.
     */
    public Explanation explain(String key, String clientId, String officeId, String shiftId) {
        Map<Scope, Object> hierarchy = new EnumMap<>(Scope.class);

        // Populate Shift
        if (clientId != null && officeId != null && shiftId != null) {
            Object val = getValueAtScope(ConfigScopeKey.shift(clientId, officeId, shiftId), key);
            if (val != null) hierarchy.put(Scope.SHIFT, val);
        }

        // Populate Office
        if (clientId != null && officeId != null) {
            Object val = getValueAtScope(ConfigScopeKey.office(clientId, officeId), key);
            if (val != null) hierarchy.put(Scope.OFFICE, val);
        }

        // Populate Client
        if (clientId != null) {
            Object val = getValueAtScope(ConfigScopeKey.client(clientId), key);
            if (val != null) hierarchy.put(Scope.CLIENT, val);
        }

        // Populate Global override or Default
        Object globalVal = getValueAtScope(ConfigScopeKey.global(), key);
        if (globalVal != null) {
            hierarchy.put(Scope.GLOBAL, globalVal);
        } else {
            SettingDefinition def = definitions.get(key);
            if (def != null && def.defaultValue() != null) {
                hierarchy.put(Scope.GLOBAL, def.defaultValue());
            }
        }

        // Determine effective value and source
        if (hierarchy.containsKey(Scope.SHIFT)) {
            return new Explanation(key, hierarchy.get(Scope.SHIFT), Scope.SHIFT, hierarchy);
        } else if (hierarchy.containsKey(Scope.OFFICE)) {
            return new Explanation(key, hierarchy.get(Scope.OFFICE), Scope.OFFICE, hierarchy);
        } else if (hierarchy.containsKey(Scope.CLIENT)) {
            return new Explanation(key, hierarchy.get(Scope.CLIENT), Scope.CLIENT, hierarchy);
        } else if (hierarchy.containsKey(Scope.GLOBAL)) {
            return new Explanation(key, hierarchy.get(Scope.GLOBAL), Scope.GLOBAL, hierarchy);
        }

        return new Explanation(key, null, Scope.GLOBAL, hierarchy);
    }

    private Object getValueAtScope(ConfigScopeKey scopeKey, String key) {
        Map<String, Object> map = overrides.get(scopeKey.toPath());
        return map != null ? map.get(key) : null;
    }

    public Map<String, SettingDefinition> getDefinitions() {
        return Map.copyOf(definitions);
    }

    public Map<String, Map<String, Object>> getOverrides() {
        return Map.copyOf(overrides);
    }
}

