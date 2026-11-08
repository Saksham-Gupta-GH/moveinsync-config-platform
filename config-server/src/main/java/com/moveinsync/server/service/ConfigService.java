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
        SettingValidator.validate(def, def.defaultValue());
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
        return publishChangeSet(scopeKey, stagedOffices, updates, author, reason, null, null, null);
    }

    public ChangeSet publishChangeSet(ConfigScopeKey scopeKey,
                                      List<String> stagedOffices,
                                      Map<String, Object> updates,
                                      String author,
                                      String reason,
                                      List<String> officeScopesToClean,
                                      Set<String> keysToClean,
                                      Long canaryVersionToClean) {
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

            // Durable commit with SQLite WAL fsync, atomically cleaning up previous canary rows if promoting
            repository.saveChangeSetAndOverrides(cs, officeScopesToClean, keysToClean, canaryVersionToClean);

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

        List<String> officeScopesToClean = new ArrayList<>();
        if (original.scopeKey().clientId() != null) {
            for (String stagedOffice : original.stagedOffices()) {
                officeScopesToClean.add(ConfigScopeKey.office(original.scopeKey().clientId(), stagedOffice).toPath());
            }
        }

        // Apply without stagedOffices constraint (promoted to entire client scope),
        // deleting only the temporary office rows the canary wrote inside the same atomic promote transaction.
        ConfigScopeKey targetScope = ConfigScopeKey.client(original.scopeKey().clientId());
        return publishChangeSet(
                targetScope,
                Collections.emptyList(),
                original.updates(),
                author,
                "Promoted from canary changeset " + changesetId,
                officeScopesToClean,
                original.updates().keySet(),
                original.version()
        );
    }

    /**
     * Rollback to a previous target version with unified diff preview.
     */
    public RollbackDiffResponse rollback(long targetVersion, boolean dryRun, String author) {
        repository.getWriterLock().lock();
        try {
            long currentVersion = repository.getLatestVersion();
            if (targetVersion < 0 || targetVersion >= currentVersion) {
                throw new IllegalArgumentException("Target version must be between 0 and " + (currentVersion - 1));
            }

            Map<String, Map<String, Object>> currentOverrides = repository.getAllOverrides();
            Map<String, Map<String, Object>> targetOverrides = reconstructStateAtVersion(targetVersion);
            Map<String, Map<String, Object>> diff = computeDiff(currentOverrides, targetOverrides);
            if (dryRun) {
                return new RollbackDiffResponse(currentVersion, targetVersion, true, diff, null);
            }

            long committedVersion = currentVersion + 1;
            Instant now = Instant.now();
            ChangeSet cs = new ChangeSet(UUID.randomUUID().toString(), committedVersion,
                    ConfigScopeKey.global(), List.of(), Map.of(), author,
                    "Rollback to V" + targetVersion, now, targetOverrides);
            repository.saveRollbackChangeSet(cs);
            sseBroadcaster.broadcast(new Delta(committedVersion, ConfigScopeKey.global(),
                    List.of(), Map.of(), false, now, targetOverrides));
            return new RollbackDiffResponse(currentVersion, targetVersion, false, diff, committedVersion);
        } finally {
            repository.getWriterLock().unlock();
        }
    }

    private Map<String, Map<String, Object>> reconstructStateAtVersion(long targetVersion) {
        Map<String, Map<String, Object>> state = new HashMap<>();
        Map<String, Map<String, Long>> writtenAt = new HashMap<>();
        Map<String, ChangeSet> byId = new HashMap<>();
        List<ChangeSet> all = repository.getChangeSetsSince(0);
        for (ChangeSet cs : all) {
            if (cs.version() > targetVersion) break;
            if (cs.replacementOverrides() != null) {
                state.clear();
                writtenAt.clear();
                cs.replacementOverrides().forEach((path, values) -> {
                    state.put(path, new HashMap<>(values));
                    Map<String, Long> versions = new HashMap<>();
                    values.keySet().forEach(key -> versions.put(key, cs.version()));
                    writtenAt.put(path, versions);
                });
            } else {
                String promotedId = "Promoted from canary changeset ";
                if (cs.reason().startsWith(promotedId)) {
                    ChangeSet canary = byId.get(cs.reason().substring(promotedId.length()));
                    if (canary != null && canary.scopeKey().clientId() != null) {
                        for (String office : canary.stagedOffices()) {
                            String path = ConfigScopeKey.office(canary.scopeKey().clientId(), office).toPath();
                            Map<String, Object> values = state.get(path);
                            Map<String, Long> versions = writtenAt.get(path);
                            if (values != null && versions != null) {
                                for (String key : canary.updates().keySet()) {
                                    if (Objects.equals(versions.get(key), canary.version())) {
                                        values.remove(key);
                                        versions.remove(key);
                                    }
                                }
                                if (values.isEmpty()) state.remove(path);
                            }
                        }
                    }
                }
                List<String> paths = cs.stagedOffices().isEmpty() ? List.of(cs.scopeKey().toPath())
                        : cs.stagedOffices().stream()
                            .map(office -> ConfigScopeKey.office(cs.scopeKey().clientId(), office).toPath()).toList();
                for (String path : paths) {
                    state.computeIfAbsent(path, ignored -> new HashMap<>()).putAll(cs.updates());
                    Map<String, Long> versions = writtenAt.computeIfAbsent(path, ignored -> new HashMap<>());
                    cs.updates().keySet().forEach(key -> versions.put(key, cs.version()));
                }
            }
            byId.put(cs.id(), cs);
        }
        return state;
    }

    private Map<String, Map<String, Object>> computeDiff(Map<String, Map<String, Object>> current, Map<String, Map<String, Object>> target) {
        Map<String, Map<String, Object>> diff = new HashMap<>();
        Set<String> paths = new HashSet<>(current.keySet());
        paths.addAll(target.keySet());
        for (String path : paths) {
            Map<String, Object> before = current.getOrDefault(path, Map.of());
            Map<String, Object> after = target.getOrDefault(path, Map.of());
            Set<String> keys = new HashSet<>(before.keySet());
            keys.addAll(after.keySet());
            Map<String, Object> changes = new HashMap<>();
            for (String key : keys) {
                if (before.containsKey(key) != after.containsKey(key)
                        || !Objects.equals(before.get(key), after.get(key))) {
                    changes.put(key, after.get(key)); // null means the override is removed.
                }
            }
            if (!changes.isEmpty()) diff.put(path, changes);
        }
        return diff;
    }

    /**
     * Retrieves an atomic snapshot filtered to only include GLOBAL settings and overrides for the requested tenant.
     * Prevents cross-tenant data leakage (e.g. Google cannot see Amazon's overrides).
     */
    public ConfigSnapshot getSnapshot(String clientId, String officeId) {
        repository.getWriterLock().lock();
        try {
            long ver = repository.getLatestVersion();
            Map<String, SettingDefinition> defs = repository.getAllDefinitions();
            Map<String, Map<String, Object>> allOverrides = repository.getAllOverrides();
            return new ConfigSnapshot(ver, defs, filterOverridesForClient(allOverrides, clientId));
        } finally {
            repository.getWriterLock().unlock();
        }
    }

    private Map<String, Map<String, Object>> filterOverridesForClient(
            Map<String, Map<String, Object>> overrides, String clientId) {
        Map<String, Map<String, Object>> filtered = new HashMap<>();
        for (var entry : overrides.entrySet()) {
            if (isScopeAllowedForClient(entry.getKey(), clientId)) {
                filtered.put(entry.getKey(), entry.getValue());
            }
        }
        return filtered;
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
            if (cs.replacementOverrides() != null) {
                deltas.add(new Delta(cs.version(), ConfigScopeKey.global(), List.of(), Map.of(), false,
                        cs.timestamp(), filterOverridesForClient(cs.replacementOverrides(), clientId)));
                continue;
            }
            boolean isTenantMatch = isTenantInScope(cs, clientId);
            boolean isOfficeMatch = isOfficeInScope(cs, officeId);

            if (isTenantMatch && isOfficeMatch) {
                deltas.add(new Delta(cs.version(), cs.scopeKey(), cs.stagedOffices(), cs.updates(), false, cs.timestamp()));
            } else {
                // Return contiguous empty delta progression with GLOBAL scope to advance client version counter without leaking tenant or office metadata
                deltas.add(Delta.emptyProgression(cs.version(), ConfigScopeKey.global()));
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

}
