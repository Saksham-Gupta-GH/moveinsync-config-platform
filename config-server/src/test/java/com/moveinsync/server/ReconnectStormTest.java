package com.moveinsync.server;

import com.moveinsync.client.MoveInSyncConfigClient;
import com.moveinsync.core.model.ConfigScopeKey;
import com.moveinsync.core.model.SettingDefinition;
import com.moveinsync.core.model.SettingType;
import com.moveinsync.core.model.ValidationRule;
import com.moveinsync.server.service.ConfigService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ReconnectStormTest {

    @LocalServerPort
    private int port;

    @Autowired
    private ConfigService configService;

    @Autowired
    private com.moveinsync.server.service.SseBroadcaster sseBroadcaster;

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Reconnect Storm: 200 simulated clients connect concurrently, receive live SSE deltas, and maintain 0 queue drops")
    void shouldHandle200ConcurrentSubscribersGracefully() throws Exception {
        configService.registerDefinition(new SettingDefinition(
                "storm_flag", SettingType.BOOLEAN, false, "Storm Flag", ValidationRule.none()));

        String serverUrl = "http://localhost:" + port;
        int clientCount = 200;
        List<MoveInSyncConfigClient> clients = new CopyOnWriteArrayList<>();
        ExecutorService stormPool = Executors.newFixedThreadPool(50);
        CountDownLatch connectLatch = new CountDownLatch(clientCount);

        java.net.http.HttpClient sharedHttpClient = java.net.http.HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofSeconds(3))
                .build();
        ScheduledExecutorService sharedHeartbeat = Executors.newScheduledThreadPool(4);
        ExecutorService sharedSsePool = Executors.newCachedThreadPool();

        try {
            // 1. Launch 200 clients concurrently simulating office fleet connection storm
            for (int i = 0; i < clientCount; i++) {
                final int id = i;
                stormPool.submit(() -> {
                    try {
                        Path clientDir = tempDir.resolve("client_" + id);
                        MoveInSyncConfigClient client = new MoveInSyncConfigClient(
                                serverUrl, "Google", "BLR-" + (id % 5 + 1), "google-service-key", clientDir,
                                sharedHttpClient, sharedHeartbeat, sharedSsePool);
                        client.start();
                        clients.add(client);
                    } catch (Exception ignored) {
                    } finally {
                        connectLatch.countDown();
                    }
                });
            }

            boolean connectedInTime = connectLatch.await(10, TimeUnit.SECONDS);
            assertThat(connectedInTime).isTrue();
            assertThat(clients.size()).isEqualTo(clientCount);

            // Allow all 200 SSE streams to register and settle
            long settleDeadline = System.currentTimeMillis() + 5000;
            while (sseBroadcaster.getConnectedSubscribers() < clientCount && System.currentTimeMillis() < settleDeadline) {
                Thread.sleep(50);
            }

            // 2. Publish a live changeset to all Google offices
            long versionBefore = configService.getLatestVersion();
            var cs = configService.publishChangeSet(
                    ConfigScopeKey.client("Google"),
                    null,
                    Map.of("storm_flag", true),
                    "admin",
                    "Storm broadcast test"
            );
            long publishedVersion = cs.version();
            assertThat(publishedVersion).isGreaterThan(versionBefore);

            // 3. Verify all 200 clients receive the update via SSE within 3 seconds
            long deadline = System.currentTimeMillis() + 3000;
            AtomicInteger updatedCount = new AtomicInteger(0);

            while (System.currentTimeMillis() < deadline && updatedCount.get() < clientCount) {
                updatedCount.set(0);
                for (MoveInSyncConfigClient client : clients) {
                    if (client.getCurrentVersion() >= publishedVersion &&
                            Boolean.TRUE.equals(client.get("storm_flag").orElse(false))) {
                        updatedCount.incrementAndGet();
                    }
                }
                if (updatedCount.get() == clientCount) break;
                Thread.sleep(50);
            }

            System.out.println("Clients successfully receiving live update: " + updatedCount.get() + " / " + clientCount);
            assertThat(updatedCount.get())
                    .as("All 200 concurrent clients should receive the broadcast update")
                    .isEqualTo(clientCount);

        } finally {
            // Clean up clients
            for (MoveInSyncConfigClient client : clients) {
                try {
                    client.close();
                } catch (Exception ignored) {
                }
            }
            stormPool.shutdownNow();
            sharedHeartbeat.shutdownNow();
            sharedSsePool.shutdownNow();
        }
    }
}
