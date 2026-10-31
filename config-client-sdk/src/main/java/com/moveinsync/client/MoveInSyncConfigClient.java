package com.moveinsync.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.moveinsync.client.cache.ConfigSnapshot;
import com.moveinsync.client.persistence.DiskCacheManager;
import com.moveinsync.core.model.Delta;
import com.moveinsync.core.model.Explanation;
import com.moveinsync.core.model.SettingDefinition;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MoveInSync High-Performance Embedded Configuration Client SDK.
 * Provides microsecond in-memory reads, disk fallback, contiguous SSE delta sync, and gap healing.
 */
public class MoveInSyncConfigClient implements AutoCloseable {
    private static final Logger log = Logger.getLogger(MoveInSyncConfigClient.class.getName());

    private final String serverBaseUrl;
    private final String clientId;
    private final String officeId;
    private final String apiKey;

    private final AtomicReference<ConfigSnapshot> snapshotRef = new AtomicReference<>(ConfigSnapshot.empty());
    private final DiskCacheManager diskCacheManager;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    private final ScheduledExecutorService heartbeatExecutor;
    private final ExecutorService sseExecutor;
    private final boolean ownsExecutors;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean sseConnected = new AtomicBoolean(false);
    private volatile InputStream activeStream;
    private Future<?> sseTask;
    private ScheduledFuture<?> heartbeatTask;

    public MoveInSyncConfigClient(String serverBaseUrl, String clientId, String officeId, String apiKey, Path storageDir) {
        this(serverBaseUrl, clientId, officeId, apiKey, storageDir, null, null, null);
    }

    public MoveInSyncConfigClient(String serverBaseUrl, String clientId, String officeId, String apiKey, Path storageDir,
                                  HttpClient httpClient, ScheduledExecutorService heartbeatExecutor, ExecutorService sseExecutor) {
        this.serverBaseUrl = serverBaseUrl.replaceAll("/$", "");
        this.clientId = clientId;
        this.officeId = officeId;
        this.apiKey = apiKey;
        this.diskCacheManager = new DiskCacheManager(storageDir);
        this.httpClient = httpClient != null ? httpClient : HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
        this.ownsExecutors = (heartbeatExecutor == null && sseExecutor == null);
        this.heartbeatExecutor = heartbeatExecutor != null ? heartbeatExecutor : Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "config-client-heartbeat");
            t.setDaemon(true);
            return t;
        });
        this.sseExecutor = sseExecutor != null ? sseExecutor : Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "config-client-sse");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Initializes the client: loads disk snapshot, syncs with server, and starts background workers.
     */
    public synchronized void start() {
        if (running.getAndSet(true)) return;

        // 1. Cold boot from disk cache if available
        diskCacheManager.loadSnapshot().ifPresent(snapshot -> {
            snapshotRef.set(snapshot);
            log.info("Hydrated initial configuration from disk snapshot (Version: " + snapshot.version() + ")");
        });

        // 2. Initial fetch from server
        tryFetchInitialSnapshot();

        // 3. Start SSE Stream Listener
        sseTask = sseExecutor.submit(this::runSseLoop);

        // Leave time within the five-second target for HTTP catch-up.
        heartbeatTask = heartbeatExecutor.scheduleAtFixedRate(this::pollHeartbeatAndHeal, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * Obtains an immutable point-in-time snapshot for multi-key consistent atomic reads.
     */
    public ConfigSnapshot getSnapshot() {
        return snapshotRef.get();
    }

    /**
     * Microsecond In-Memory Read. Zero network calls, zero locks.
     */
    public Optional<Object> get(String key) {
        return get(key, null);
    }

    public Optional<Object> get(ConfigSnapshot snapshot, String key) {
        return snapshot.resolve(key, clientId, officeId, null);
    }

    /** Typed read from a captured snapshot, preserving multi-key consistency. */
    public <T> T get(ConfigSnapshot snapshot, String key, Class<T> type) {
        Object value = get(snapshot, key).orElseThrow(() ->
                new IllegalArgumentException("Unknown configuration key: " + key));
        return type.cast(value);
    }

    public Optional<Object> get(String key, String shiftId) {
        return snapshotRef.get().resolve(key, clientId, officeId, shiftId);
    }

    public Explanation explain(String key, String shiftId) {
        return snapshotRef.get().explain(key, clientId, officeId, shiftId);
    }

    public boolean isSseConnected() {
        return sseConnected.get();
    }

    public long getCurrentVersion() {
        return snapshotRef.get().version();
    }

    /**
     * Handles incoming delta with contiguous versioning and gap detection.
     */
    public synchronized void handleDelta(Delta delta) {
        long currentVer = snapshotRef.get().version();

        // Stale or duplicate delta -> discard as no-op
        if (delta.version() <= currentVer) {
            return;
        }

        // Strictly contiguous progression (v == current + 1)
        if (delta.version() == currentVer + 1) {
            ConfigSnapshot updated;
            if (delta.empty() || delta.changes().isEmpty()) {
                updated = snapshotRef.get().withVersion(delta.version());
            } else {
                updated = snapshotRef.get().applyDelta(delta.version(), delta.scopeKey(), delta.changes());
            }
            snapshotRef.set(updated);
            diskCacheManager.saveSnapshot(updated);
        } else {
            // Gap detected! (v > current + 1) -> Pull missing contiguous deltas
            log.warning("Detected version gap: received V" + delta.version() + " but client is at V" + currentVer + ". Triggering delta sync.");
            fetchMissingDeltas(currentVer);
        }
    }

    private void tryFetchInitialSnapshot() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(serverBaseUrl + "/api/v1/config/snapshot?client_id=" + clientId + "&office_id=" + officeId))
                    .header("X-API-Key", apiKey)
                    .timeout(Duration.ofSeconds(4))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                ConfigSnapshot snapshot = mapper.readValue(response.body(), ConfigSnapshot.class);
                snapshotRef.set(snapshot);
                diskCacheManager.saveSnapshot(snapshot);
                log.info("Successfully fetched remote snapshot. Active Version: " + snapshot.version());
            }
        } catch (Exception e) {
            log.warning("Unable to fetch initial snapshot from config server: " + e.getMessage() + ". Continuing with local snapshot.");
        }
    }

    private void pollHeartbeatAndHeal() {
        if (!running.get()) return;
        try {
            long currentVer = snapshotRef.get().version();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(serverBaseUrl + "/api/v1/config/version?client_id=" + clientId + "&office_id=" + officeId + "&client_version=" + currentVer))
                    .header("X-API-Key", apiKey)
                    .timeout(Duration.ofSeconds(3))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                Map<String, Object> body = mapper.readValue(response.body(), new TypeReference<>() {});
                long serverVersion = ((Number) body.get("version")).longValue();

                if (serverVersion > currentVer) {
                    log.info("Heartbeat detected server ahead (Client: " + currentVer + ", Server: " + serverVersion + "). Syncing deltas.");
                    fetchMissingDeltas(currentVer);
                }
            }
        } catch (Exception e) {
            log.fine("Heartbeat failed (server might be down): " + e.getMessage());
        }
    }

    private synchronized void fetchMissingDeltas(long sinceVersion) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(serverBaseUrl + "/api/v1/config/deltas?client_id=" + clientId + "&office_id=" + officeId + "&since_version=" + sinceVersion))
                    .header("X-API-Key", apiKey)
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                List<Delta> deltas = mapper.readValue(response.body(), new TypeReference<>() {});
                for (Delta d : deltas) {
                    handleDelta(d);
                }
            }
        } catch (Exception e) {
            log.warning("Failed to fetch deltas since V" + sinceVersion + ": " + e.getMessage());
        }
    }

    private void runSseLoop() {
        long previousDelayMs = 50;

        while (running.get()) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(serverBaseUrl + "/api/v1/config/stream?client_id=" + clientId + "&office_id=" + officeId))
                        .header("X-API-Key", apiKey)
                        .header("Accept", "text/event-stream")
                        .timeout(Duration.ofHours(1))
                        .GET()
                        .build();

                HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
                if (response.statusCode() == 200) {
                    activeStream = response.body();
                    sseConnected.set(true);
                    // Subscribe first, then replay changes missed while disconnected.
                    fetchMissingDeltas(getCurrentVersion());
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.body()))) {
                        String line;
                        while (running.get() && (line = reader.readLine()) != null) {
                            if (line.startsWith("data:")) {
                                String json = line.substring(5).trim();
                                if (!json.isEmpty()) {
                                    Delta delta = mapper.readValue(json, Delta.class);
                                    handleDelta(delta);
                                }
                            }
                        }
                    }
                } else {
                    response.body().close();
                }
            } catch (Exception e) {
                if (!running.get()) break;
            } finally {
                activeStream = null;
                sseConnected.set(false);
            }
            // Back off after EOF and rejected requests as well as network exceptions.
            if (!running.get()) break;
            previousDelayMs = Math.min(2000,
                    ThreadLocalRandom.current().nextLong(50, previousDelayMs * 3 + 1));
            try {
                Thread.sleep(previousDelayMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Override
    public void close() {
        if (!running.getAndSet(false)) return;
        sseConnected.set(false);
        if (heartbeatTask != null) heartbeatTask.cancel(true);
        if (sseTask != null) sseTask.cancel(true);
        InputStream stream = activeStream;
        if (stream != null) {
            try { stream.close(); } catch (java.io.IOException ignored) { }
        }
        if (ownsExecutors) {
            heartbeatExecutor.shutdownNow();
            sseExecutor.shutdownNow();
        }
        diskCacheManager.saveSnapshot(snapshotRef.get());
    }
}
