package com.moveinsync.client.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.moveinsync.client.cache.ConfigSnapshot;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages atomic disk persistence of the configuration snapshot to guarantee cold-boot survival.
 */
public class DiskCacheManager {
    private static final Logger log = Logger.getLogger(DiskCacheManager.class.getName());
    private final Path snapshotFilePath;
    private final ObjectMapper mapper;

    public DiskCacheManager(Path storageDirectory) {
        try {
            Files.createDirectories(storageDirectory);
        } catch (IOException e) {
            log.warning("Could not create storage directory: " + e.getMessage());
        }
        this.snapshotFilePath = storageDirectory.resolve("snapshot.json");
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
    }

    public synchronized void saveSnapshot(ConfigSnapshot snapshot) {
        try {
            Path tempFile = Files.createTempFile(snapshotFilePath.getParent(), "snapshot-", ".tmp");
            mapper.writeValue(tempFile.toFile(), snapshot);

            // Atomic rename to avoid partial reads on crash
            Files.move(tempFile, snapshotFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log.log(Level.WARNING, "Failed to persist snapshot to disk: " + e.getMessage(), e);
        }
    }

    public synchronized Optional<ConfigSnapshot> loadSnapshot() {
        if (!Files.exists(snapshotFilePath)) {
            return Optional.empty();
        }
        try {
            ConfigSnapshot snapshot = mapper.readValue(snapshotFilePath.toFile(), ConfigSnapshot.class);
            return Optional.of(snapshot);
        } catch (IOException e) {
            log.log(Level.WARNING, "Failed to read snapshot from disk cache: " + e.getMessage(), e);
            return Optional.empty();
        }
    }
}
