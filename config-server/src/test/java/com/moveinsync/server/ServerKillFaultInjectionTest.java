package com.moveinsync.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ServerKillFaultInjectionTest {

    @TempDir
    Path tempDir;

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    private int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private Process startServerProcess(int port, File dbFile, String logName) throws IOException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "-cp", classpath,
                "com.moveinsync.server.ConfigServerApplication",
                "--server.port=" + port,
                "--spring.datasource.url=jdbc:sqlite:" + dbFile.getAbsolutePath()
        );
        File logFile = tempDir.resolve(logName).toFile();
        pb.redirectOutput(logFile);
        pb.redirectError(logFile);
        return pb.start();
    }

    private void waitForServerReady(int port) throws Exception {
        String url = "http://localhost:" + port + "/api/v1/config/version?client_id=Google";
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("X-API-Key", "admin-secret-key")
                        .GET()
                        .build();
                HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) {
                    return; // Ready
                }
            } catch (Exception ignored) {
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("Server failed to start within 15 seconds on port " + port);
    }

    @Test
    @DisplayName("Durability & Crash Recovery: Real SIGKILL (kill -9) preserves committed changesets upon restart via SQLite WAL synchronous=FULL")
    void shouldPersistCommittedChangeAcrossHardProcessKill() throws Exception {
        int port = findFreePort();
        File dbFile = tempDir.resolve("kill_test.db").toFile();

        // 1. Start Server Process #1
        Process process1 = startServerProcess(port, dbFile, "server1.log");
        try {
            waitForServerReady(port);

            // 2. Register Definition
            String regPayload = mapper.writeValueAsString(Map.of(
                    "key", "durability_cutoff",
                    "type", "NUMBER",
                    "defaultValue", 15,
                    "description", "Cutoff before kill",
                    "validationRule", Map.of("type", "NONE")
            ));
            HttpRequest regReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/api/v1/config/definitions"))
                    .header("Content-Type", "application/json")
                    .header("X-API-Key", "admin-secret-key")
                    .POST(HttpRequest.BodyPublishers.ofString(regPayload))
                    .build();
            HttpResponse<String> regResp = httpClient.send(regReq, HttpResponse.BodyHandlers.ofString());
            assertThat(regResp.statusCode()).isIn(200, 201);

            // 3. Commit a changeset just before crash
            String csPayload = mapper.writeValueAsString(Map.of(
                    "scopeKey", Map.of("scope", "CLIENT", "clientId", "Google"),
                    "updates", Map.of("durability_cutoff", 999),
                    "author", "admin",
                    "reason", "Pre-kill durable commit"
            ));
            HttpRequest csReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/api/v1/config/changeset"))
                    .header("Content-Type", "application/json")
                    .header("X-API-Key", "admin-secret-key")
                    .POST(HttpRequest.BodyPublishers.ofString(csPayload))
                    .build();
            HttpResponse<String> csResp = httpClient.send(csReq, HttpResponse.BodyHandlers.ofString());
            assertThat(csResp.statusCode()).isEqualTo(201);

            JsonNode csNode = mapper.readTree(csResp.body());
            long committedVersion = csNode.get("version").asLong();
            assertThat(committedVersion).isGreaterThan(0);

            // 4. HARD KILL PROCESS (SIGKILL - kill -9)
            // No graceful shutdown, no Spring context stop
            process1.destroyForcibly();
            boolean killed = process1.waitFor(5, TimeUnit.SECONDS);
            assertThat(killed).isTrue();
            assertThat(process1.isAlive()).isFalse();

        } finally {
            if (process1.isAlive()) {
                process1.destroyForcibly();
            }
        }

        // 5. RESTART: Start Server Process #2 pointing to SAME SQLite database
        Process process2 = startServerProcess(port, dbFile, "server2.log");
        try {
            waitForServerReady(port);

            // 6. Query snapshot from restarted server
            HttpRequest snapReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/api/v1/config/snapshot?client_id=Google&office_id=BLR-1"))
                    .header("X-API-Key", "google-service-key")
                    .GET()
                    .build();
            HttpResponse<String> snapResp = httpClient.send(snapReq, HttpResponse.BodyHandlers.ofString());
            assertThat(snapResp.statusCode()).isEqualTo(200);

            com.moveinsync.client.cache.ConfigSnapshot snap = mapper.readValue(snapResp.body(), com.moveinsync.client.cache.ConfigSnapshot.class);

            // 7. PROVEN: The committed change survived the hard crash!
            assertThat(snap.version()).isGreaterThanOrEqualTo(1);
            assertThat(snap.resolve("durability_cutoff", "Google", "BLR-1", null)).contains(999);

        } finally {
            if (process2.isAlive()) {
                process2.destroyForcibly();
            }
        }
    }
}
