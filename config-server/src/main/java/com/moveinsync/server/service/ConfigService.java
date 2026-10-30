package com.moveinsync.server.service;

import com.moveinsync.client.cache.ConfigSnapshot;
import com.moveinsync.core.model.*;
import com.moveinsync.core.validator.SettingValidator;
import com.moveinsync.core.validator.ValidationException;
import com.moveinsync.server.repository.SqliteConfigRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Core Application Service managing atomic changesets, staged canary rollout,
 * unified rollback with diffs, tenant isolation data filtering, and operational metrics.
 */
@Service
public class ConfigService {
    private static final Logger log = Logger.getLogger(ConfigService.class.getName());

    private final SqliteConfigRepository repository;
    private final SseBroadcaster sseBroadcaster;

    // Operational telemetry metrics
    private final AtomicLong rejectionCount = new AtomicLong(0);
    private final AtomicLong maxVersionLagSeen = new AtomicLong(0);
    private final AtomicLong clientCatchupRequestsTotal = new AtomicLong(0);
    private final AtomicLong errorRequestsTotal = new AtomicLong(0);

    public record RollbackDiffResponse(
            long currentVersion,
            long targetVersion,
            boolean dryRun,
            Map<String, Map<String, Object>> diff,
            Long committedVersion
    ) {}

    public ConfigService(SqliteConfigRepository repository, SseBroadcaster sseBroadcaster) {
        this.repository = repository;
        this.sseBroadcaster = sseBroadcaster;
    }

    public void registerDefinition(SettingDefinition def) {
        repository.saveDefinition(def);
    }

    public Map<String, SettingDefinition> getDefinitions() {
        return repository.getAllDefinitions();
    }

    /**
     * Publishes an atomic multi-key changeset under a single writer lock.
     */
    public ChangeSet publishChangeSet(ConfigScopeKey scopeKey,
                                      List<String> stagedOffices,
                                      Map<String, Object> updates,
                                      String author,
                                      String reason) {
        // 1. Validation phase (If ANY key fails validation, the entire changeset is rejected)
        Map<String, SettingDefinition> defs = repository.getAllDefinitions();
        for (Map.Entry<String, Object> entry : updates.entrySet()) {
            SettingDefinition def = defs.get(entry.getKey());
            if (def == null) {
                rejectionCount.incrementAndGet();
                throw new ValidationException(entry.getKey(), "Setting definition does not exist");
            }
            try {
                SettingValidator.validate(def, entry.getValue());
            } catch (ValidationException ve) {
                rejectionCount.incrementAndGet();
                throw ve;
            }
        }

        // 2. Atomic Commit & Broadcast under Writer Lock
        repository.getWriterLock().lock();
        try {
            long nextVersion = repository.getLatestVersion() + 1;
            String id = UUID.randomUUID().toString();
            ChangeSet cs = new ChangeSet(id, nextVersion, scopeKey, stagedOffices, updates, author, reason, Instant.now());

            // Durable commit with SQLite WAL fsync
            repository.saveChangeSetAndOverrides(cs);

            // Broadcast delta across SSE in strict monotonic order
            Delta delta = new Delta(nextVersion, scopeKey, stagedOffices, updates, false, Instant.now());
            sseBroadcaster.broadcast(delta);

            log.info("Committed and broadcast ChangeSet V" + nextVersion + " [id=" + id + "] with " + updates.size() + " keys.");
            return cs;
        } finally {
            repository.getWriterLock().unlock();
        }
    }

    /**
     * Promotes a canary staged changeset to all remaining offices under the client.
     */
    public ChangeSet promoteChangeSet(String changesetId, String author) {
        ChangeSet original = repository.getChangeSetById(changesetId)
                .orElseThrow(() -> new IllegalArgumentException("Changeset not found: " + changesetId));

        if (original.stagedOffices() == null || original.stagedOffices().isEmpty()) {
            throw new IllegalArgumentException("Changeset is already global/non-staged: " + changesetId);
        }

        // Apply without stagedOffices constraint (promoted to entire client scope)
        ConfigScopeKey targetScope = ConfigScopeKey.client(original.scopeKey().clientId());
        return publishChangeSet(targetScope, Collections.emptyList(), original.updates(), author, "Promoted from canary changeset " + changesetId);
    }

    /**
     * Rollback to a previous target version with unified diff preview.
     */
    public RollbackDiffResponse rollback(long targetVersion, boolean dryRun, String author) {
        long currentVersion = repository.getLatestVersion();
        if (targetVersion >= currentVersion) {
            throw new IllegalArgumentException("Target version (" + targetVersion + ") must be less than current version (" + currentVersion + ")");
        }

        // Compute state at target version vs current state
        Map<String, Map<String, Object>> currentOverrides = repository.getAllOverrides();
        Map<String, Map<String, Object>> targetOverrides = reconstructStateAtVersion(targetVersion);

        Map<String, Map<String, Object>> diff = computeDiff(currentOverrides, targetOverrides);

        if (dryRun) {
            return new RollbackDiffResponse(currentVersion, targetVersion, true, diff, null);
        }

        // Apply inverted changes under new forward version
        Long newCommittedVersion = null;
        for (Map.Entry<String, Map<String, Object>> entry : diff.entrySet()) {
            ConfigScopeKey scopeKey = parseScopePath(entry.getKey());
            ChangeSet cs = publishChangeSet(scopeKey, Collections.emptyList(), entry.getValue(), author, "Rollback to V" + targetVersion);
            newCommittedVersion = cs.version();
        }

        return new RollbackDiffResponse(currentVersion, targetVersion, false, diff, newCommittedVersion);
    }

    private Map<String, Map<String, Object>> reconstructStateAtVersion(long targetVersion) {
        Map<String, Map<String, Object>> state = new HashMap<>();
        List<ChangeSet> all = repository.getChangeSetsSince(0);
        for (ChangeSet cs : all) {
            if (cs.version() <= targetVersion) {
                Map<String, Object> scopeMap = state.computeIfAbsent(cs.scopeKey().toPath(), k -> new HashMap<>());
                scopeMap.putAll(cs.updates());
            }
        }
        return state;
    }

    private Map<String, Map<String, Object>> computeDiff(Map<String, Map<String, Object>> current, Map<String, Map<String, Object>> target) {
        Map<String, Map<String, Object>> diff = new HashMap<>();
        Map<String, SettingDefinition> defs = repository.getAllDefinitions();

        // 1. Process target scopes and compare with current
        for (Map.Entry<String, Map<String, Object>> targetScope : target.entrySet()) {
            String path = targetScope.getKey();
            Map<String, Object> targetMap = targetScope.getValue();
            Map<String, Object> currentMap = current.getOrDefault(path, Collections.emptyMap());

            Map<String, Object> scopeDiff = new HashMap<>();
            for (Map.Entry<String, Object> valEntry : targetMap.entrySet()) {
                Object curVal = currentMap.get(valEntry.getKey());
                if (!Objects.equals(curVal, valEntry.getValue())) {
                    scopeDiff.put(valEntry.getKey(), valEntry.getValue());
                }
            }

            // Keys added in current but not present at target version: reset to definition default
            for (String curKey : currentMap.keySet()) {
                if (!targetMap.containsKey(curKey)) {
                    SettingDefinition def = defs.get(curKey);
                    if (def != null && def.defaultValue() != null) {
                        scopeDiff.put(curKey, def.defaultValue());
                    }
                }
            }

            if (!scopeDiff.isEmpty()) {
                diff.put(path, scopeDiff);
            }
        }

        // 2. Scopes that exist in current but were not created yet at target version
        for (Map.Entry<String, Map<String, Object>> curScope : current.entrySet()) {
            String path = curScope.getKey();
            if (!target.containsKey(path)) {
                Map<String, Object> scopeDiff = new HashMap<>();
                for (String curKey : curScope.getValue().keySet()) {
                    SettingDefinition def = defs.get(curKey);
                    if (def != null && def.defaultValue() != null) {
                        scopeDiff.put(curKey, def.defaultValue());
                    }
                }
                if (!scopeDiff.isEmpty()) {
                    diff.put(path, scopeDiff);
                }
            }
        }

        return diff;
    }

    /**
     * Retrieves an atomic snapshot filtered to only include GLOBAL settings and overrides for the requested tenant.
     * Prevents cross-tenant data leakage (e.g. Google cannot see Amazon's overrides).
     */
    public ConfigSnapshot getSnapshot(String clientId, String officeId) {
        long ver = repository.getLatestVersion();
        Map<String, SettingDefinition> defs = repository.getAllDefinitions();
        Map<String, Map<String, Object>> allOverrides = repository.getAllOverrides();

        Map<String, Map<String, Object>> filteredOverrides = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : allOverrides.entrySet()) {
            String scopePath = entry.getKey();
            if (isScopeAllowedForClient(scopePath, clientId)) {
                filteredOverrides.put(scopePath, entry.getValue());
            }
        }
        return new ConfigSnapshot(ver, defs, filteredOverrides);
    }

    private boolean isScopeAllowedForClient(String scopePath, String clientId) {
        if ("GLOBAL".equalsIgnoreCase(scopePath)) {
            return true;
        }
        if (clientId == null) {
            return false;
        }
        String[] parts = scopePath.split(":", 2);
        if (parts.length < 2) return false;
        String[] subParts = parts[1].split("/");
        return subParts[0].equalsIgnoreCase(clientId);
    }

    /**
     * Retrieves deltas with strict tenant filtering.
     * If a delta belongs to another tenant or out-of-scope office, returns an empty progression
     * so the client's local version sequence advances monotonically without leaking data or triggering gaps.
     */
    public List<Delta> getDeltas(String clientId, String officeId, long sinceVersion) {
        if (sinceVersion > 0) {
            clientCatchupRequestsTotal.incrementAndGet();
        }
        List<ChangeSet> changes = repository.getChangeSetsSince(sinceVersion);
        List<Delta> deltas = new ArrayList<>();
        for (ChangeSet cs : changes) {
            boolean isTenantMatch = isTenantInScope(cs, clientId);
            boolean isOfficeMatch = isOfficeInScope(cs, officeId);

            if (isTenantMatch && isOfficeMatch) {
                deltas.add(new Delta(cs.version(), cs.scopeKey(), cs.stagedOffices(), cs.updates(), false, cs.timestamp()));
            } else {
                // Return contiguous empty delta progression to advance client version counter without leaking payload
                deltas.add(Delta.emptyProgression(cs.version(), cs.scopeKey()));
            }
        }
        return deltas;
    }

    private boolean isTenantInScope(ChangeSet cs, String clientId) {
        String targetClient = cs.scopeKey().clientId();
        if (targetClient == null) {
            return true; // Global changes apply to all clients
        }
        return clientId != null && targetClient.equalsIgnoreCase(clientId);
    }

    private boolean isOfficeInScope(ChangeSet cs, String officeId) {
        if (cs.stagedOffices() == null || cs.stagedOffices().isEmpty()) return true;
        return officeId != null && cs.stagedOffices().contains(officeId);
    }

    public long getLatestVersion() {
        return repository.getLatestVersion();
    }

    public long getRejectionCount() {
        return rejectionCount.get();
    }

    public long getMaxVersionLagSeen() {
        return maxVersionLagSeen.get();
    }

    public void recordLag(long lag) {
        maxVersionLagSeen.accumulateAndGet(lag, Math::max);
    }

    public long getClientCatchupRequestsTotal() {
        return clientCatchupRequestsTotal.get();
    }

    public long getErrorRequestsTotal() {
        return errorRequestsTotal.get();
    }

    public void incrementErrorCount() {
        errorRequestsTotal.incrementAndGet();
    }

    private ConfigScopeKey parseScopePath(String path) {
        if ("GLOBAL".equalsIgnoreCase(path)) return ConfigScopeKey.global();
        String[] parts = path.split(":", 2);
        Scope scope = Scope.valueOf(parts[0]);
        String[] subParts = parts[1].split("/");
        return switch (scope) {
            case GLOBAL -> ConfigScopeKey.global();
            case CLIENT -> ConfigScopeKey.client(subParts[0]);
            case OFFICE -> ConfigScopeKey.office(subParts[0], subParts[1]);
            case SHIFT -> ConfigScopeKey.shift(subParts[0], subParts[1], subParts[2]);
        };
    }
}
