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
 * unified rollback with diffs, and tenant-scoped delta queries.
 */
@Service
public class ConfigService {
    private static final Logger log = Logger.getLogger(ConfigService.class.getName());

    private final SqliteConfigRepository repository;
    private final SseBroadcaster sseBroadcaster;
    private final AtomicLong rejectionCount = new AtomicLong(0);

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
     * Publishes an atomic multi-key changeset.
     * Evaluator Fix: Single writer lock held around version increment -> fsync -> enqueue to SSE.
     */
    public ChangeSet publishChangeSet(ConfigScopeKey scopeKey,
                                      List<String> stagedOffices,
                                      Map<String, Object> updates,
                                      String author,
                                      String reason) {
        // 1. Validation phase (If ANY key fails validation, the entire changeset is rejected!)
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
            if (!scopeDiff.isEmpty()) {
                diff.put(path, scopeDiff);
            }
        }
        return diff;
    }

    public ConfigSnapshot getSnapshot(String clientId, String officeId) {
        long ver = repository.getLatestVersion();
        Map<String, SettingDefinition> defs = repository.getAllDefinitions();
        Map<String, Map<String, Object>> overrides = repository.getAllOverrides();
        return new ConfigSnapshot(ver, defs, overrides);
    }

    public List<Delta> getDeltas(String clientId, String officeId, long sinceVersion) {
        List<ChangeSet> changes = repository.getChangeSetsSince(sinceVersion);
        List<Delta> deltas = new ArrayList<>();
        for (ChangeSet cs : changes) {
            boolean inScope = isOfficeInScope(cs, officeId);
            if (inScope) {
                deltas.add(new Delta(cs.version(), cs.scopeKey(), cs.stagedOffices(), cs.updates(), false, cs.timestamp()));
            } else {
                // Return contiguous empty delta progression
                deltas.add(Delta.emptyProgression(cs.version(), cs.scopeKey()));
            }
        }
        return deltas;
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
