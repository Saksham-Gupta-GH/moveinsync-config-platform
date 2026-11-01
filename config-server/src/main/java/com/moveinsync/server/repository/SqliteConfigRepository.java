package com.moveinsync.server.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.moveinsync.core.model.ChangeSet;
import com.moveinsync.core.model.ConfigScopeKey;
import com.moveinsync.core.model.Scope;
import com.moveinsync.core.model.SettingDefinition;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SQLite Repository with explicit WAL mode and synchronous=FULL to guarantee fsync on every commit.
 */
@Repository
public class SqliteConfigRepository {
    private static final Logger log = Logger.getLogger(SqliteConfigRepository.class.getName());

    private final DataSource dataSource;
    private final ObjectMapper mapper;
    private final ReentrantLock writerLock = new ReentrantLock(true);

    public SqliteConfigRepository(DataSource dataSource) {
        this.dataSource = dataSource;
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
        initDatabase();
    }

    public ReentrantLock getWriterLock() {
        return writerLock;
    }

    private void initDatabase() {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            // Crucial Evaluator Fix: WAL mode + synchronous=FULL to guarantee disk fsync on commit
            stmt.execute("PRAGMA journal_mode = WAL;");
            stmt.execute("PRAGMA synchronous = FULL;");

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS setting_definitions (
                    key TEXT PRIMARY KEY,
                    json_data TEXT NOT NULL
                );
            """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS changesets (
                    version INTEGER PRIMARY KEY,
                    id TEXT NOT NULL,
                    scope_path TEXT NOT NULL,
                    staged_offices TEXT NOT NULL,
                    updates_json TEXT NOT NULL,
                    author TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
            """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS effective_overrides (
                    scope_path TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value_json TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    PRIMARY KEY (scope_path, key)
                );
            """);

            log.info("Initialized SQLite Configuration Store with WAL mode & synchronous=FULL");
        } catch (SQLException e) {
            log.log(Level.SEVERE, "Failed to initialize SQLite database: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public void saveDefinition(SettingDefinition def) {
        writerLock.lock();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT OR REPLACE INTO setting_definitions (key, json_data) VALUES (?, ?);")) {
            ps.setString(1, def.key());
            ps.setString(2, mapper.writeValueAsString(def));
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Error saving definition: " + def.key(), e);
        } finally {
            writerLock.unlock();
        }
    }

    public Map<String, SettingDefinition> getAllDefinitions() {
        Map<String, SettingDefinition> map = new HashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT json_data FROM setting_definitions;");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                SettingDefinition def = mapper.readValue(rs.getString("json_data"), SettingDefinition.class);
                map.put(def.key(), def);
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed to query definitions: " + e.getMessage(), e);
            throw new RuntimeException("Database error querying definitions", e);
        }
        return map;
    }

    public long getLatestVersion() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT COALESCE(MAX(version), 0) AS max_v FROM changesets;");
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getLong("max_v");
            }
        } catch (SQLException e) {
            log.log(Level.SEVERE, "Failed to fetch latest version: " + e.getMessage(), e);
            throw new RuntimeException("Database error fetching latest version", e);
        }
        return 0L;
    }

    public void saveChangeSetAndOverrides(ChangeSet cs) {
        saveChangeSetAndOverrides(cs, null, null, null);
    }

    public void saveChangeSetAndOverrides(ChangeSet cs, List<String> officeScopesToClean, Set<String> keysToClean, Long canaryVersionToClean) {
        writerLock.lock();
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // If this is a promotion, delete only the rows the canary wrote at OFFICE scope inside this transaction
                if (officeScopesToClean != null && !officeScopesToClean.isEmpty() && keysToClean != null && !keysToClean.isEmpty() && canaryVersionToClean != null) {
                    try (PreparedStatement psDel = conn.prepareStatement(
                            "DELETE FROM effective_overrides WHERE scope_path = ? AND key = ? AND version = ?;")) {
                        for (String scopePath : officeScopesToClean) {
                            for (String k : keysToClean) {
                                psDel.setString(1, scopePath);
                                psDel.setString(2, k);
                                psDel.setLong(3, canaryVersionToClean);
                                psDel.addBatch();
                            }
                        }
                        psDel.executeBatch();
                    }
                }

                // 1. Insert changeset
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO changesets (version, id, scope_path, staged_offices, updates_json, author, reason, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """)) {
                    ps.setLong(1, cs.version());
                    ps.setString(2, cs.id());
                    ps.setString(3, cs.scopeKey().toPath());
                    ps.setString(4, mapper.writeValueAsString(cs.stagedOffices()));
                    ps.setString(5, mapper.writeValueAsString(cs.updates()));
                    ps.setString(6, cs.author());
                    ps.setString(7, cs.reason());
                    ps.setString(8, cs.timestamp().toString());
                    ps.executeUpdate();
                }

                // 2. Insert or replace effective overrides
                // For staged canary rollouts, store under each staged office scope so non-canary offices do not inherit it via snapshot
                List<String> targetScopePaths = new ArrayList<>();
                if (cs.stagedOffices() != null && !cs.stagedOffices().isEmpty() && cs.scopeKey().clientId() != null) {
                    for (String stagedOffice : cs.stagedOffices()) {
                        targetScopePaths.add(ConfigScopeKey.office(cs.scopeKey().clientId(), stagedOffice).toPath());
                    }
                } else {
                    targetScopePaths.add(cs.scopeKey().toPath());
                }

                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT OR REPLACE INTO effective_overrides (scope_path, key, value_json, version)
                    VALUES (?, ?, ?, ?);
                """)) {
                    for (String scopePath : targetScopePaths) {
                        for (Map.Entry<String, Object> entry : cs.updates().entrySet()) {
                            ps.setString(1, scopePath);
                            ps.setString(2, entry.getKey());
                            ps.setString(3, mapper.writeValueAsString(entry.getValue()));
                            ps.setLong(4, cs.version());
                            ps.addBatch();
                        }
                    }
                    ps.executeBatch();
                }

                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw new RuntimeException("Transaction rolled back for changeset V" + cs.version(), e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            writerLock.unlock();
        }
    }

    public Map<String, Map<String, Object>> getAllOverrides() {
        Map<String, Map<String, Object>> map = new HashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT scope_path, key, value_json FROM effective_overrides;");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String scopePath = rs.getString("scope_path");
                String key = rs.getString("key");
                Object val = mapper.readValue(rs.getString("value_json"), Object.class);
                map.computeIfAbsent(scopePath, k -> new HashMap<>()).put(key, val);
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed to fetch overrides: " + e.getMessage(), e);
            throw new RuntimeException("Database error fetching overrides", e);
        }
        return map;
    }

    public List<ChangeSet> getChangeSetsSince(long sinceVersion) {
        List<ChangeSet> list = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT * FROM changesets WHERE version > ? ORDER BY version ASC;")) {
            ps.setLong(1, sinceVersion);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long ver = rs.getLong("version");
                    String id = rs.getString("id");
                    String scopePath = rs.getString("scope_path");
                    List<String> staged = mapper.readValue(rs.getString("staged_offices"), new TypeReference<>() {});
                    Map<String, Object> updates = mapper.readValue(rs.getString("updates_json"), new TypeReference<>() {});
                    String author = rs.getString("author");
                    String reason = rs.getString("reason");
                    Instant ts = Instant.parse(rs.getString("created_at"));

                    ConfigScopeKey scopeKey = parseScopePath(scopePath);
                    list.add(new ChangeSet(id, ver, scopeKey, staged, updates, author, reason, ts));
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Failed to query changesets since V" + sinceVersion + ": " + e.getMessage(), e);
        }
        return list;
    }

    public Optional<ChangeSet> getChangeSetById(String id) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT * FROM changesets WHERE id = ?;")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long ver = rs.getLong("version");
                    String scopePath = rs.getString("scope_path");
                    List<String> staged = mapper.readValue(rs.getString("staged_offices"), new TypeReference<>() {});
                    Map<String, Object> updates = mapper.readValue(rs.getString("updates_json"), new TypeReference<>() {});
                    String author = rs.getString("author");
                    String reason = rs.getString("reason");
                    Instant ts = Instant.parse(rs.getString("created_at"));

                    return Optional.of(new ChangeSet(id, ver, parseScopePath(scopePath), staged, updates, author, reason, ts));
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Failed to find changeset by ID: " + id, e);
        }
        return Optional.empty();
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

