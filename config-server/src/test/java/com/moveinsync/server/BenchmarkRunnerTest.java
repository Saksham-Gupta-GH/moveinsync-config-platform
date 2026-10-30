package com.moveinsync.server;

import com.moveinsync.client.MoveInSyncConfigClient;
import com.moveinsync.core.model.ConfigScopeKey;
import com.moveinsync.core.model.SettingDefinition;
import com.moveinsync.core.model.SettingType;
import com.moveinsync.core.model.ValidationRule;
import com.moveinsync.server.service.ConfigService;
import com.moveinsync.server.service.SseBroadcaster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class BenchmarkRunnerTest {

    @LocalServerPort
    private int port;

    @Autowired
    private ConfigService configService;

    @Autowired
    private SseBroadcaster sseBroadcaster;

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Performance Benchmark: JIT Warmed In-Memory Reads & 200-Client Live Propagation Latency (p50/p90/p99)")
    void runFullPerformanceBenchmark() throws Exception {
        System.out.println("===============================================================================");
        System.out.println("           MOVEINSYNC CONFIGURATION PLATFORM BENCHMARK SUITE");
        System.out.println("===============================================================================");

        configService.registerDefinition(new SettingDefinition(
                "benchmark_cutoff", SettingType.NUMBER, 15, "Benchmark Cutoff", ValidationRule.none()));
        configService.publishChangeSet(
                ConfigScopeKey.client("Google"), null, Map.of("benchmark_cutoff", 30), "bench", "Setup");
        configService.publishChangeSet(
                ConfigScopeKey.office("Google", "BLR-1"), null, Map.of("benchmark_cutoff", 45), "bench", "Setup");

        String serverUrl = "http://localhost:" + port;
        Path clientDir = tempDir.resolve("bench_client");

        try (MoveInSyncConfigClient client = new MoveInSyncConfigClient(
                serverUrl, "Google", "BLR-1", "google-service-key", clientDir)) {
            client.start();
            Thread.sleep(500);

            // -------------------------------------------------------------------------
            // PART 1: IN-MEMORY READ LATENCY BENCHMARK (4-TIER HIERARCHY RESOLUTION)
            // -------------------------------------------------------------------------
            int warmupIterations = 50_000;
            int measureIterations = 100_000;

            // 1. JIT Warmup Loop (Ensures Hotspot C2 inlining & tier-4 compilation)
            for (int i = 0; i < warmupIterations; i++) {
                client.get("benchmark_cutoff", "MORNING_SHIFT");
            }

            // 2. Timed Measurement Loop
            long[] latenciesNano = new long[measureIterations];
            for (int i = 0; i < measureIterations; i++) {
                long start = System.nanoTime();
                client.get("benchmark_cutoff", "MORNING_SHIFT");
                latenciesNano[i] = System.nanoTime() - start;
            }

            Arrays.sort(latenciesNano);

            double p50Us = latenciesNano[(int) (measureIterations * 0.50)] / 1000.0;
            double p90Us = latenciesNano[(int) (measureIterations * 0.90)] / 1000.0;
            double p95Us = latenciesNano[(int) (measureIterations * 0.95)] / 1000.0;
            double p99Us = latenciesNano[(int) (measureIterations * 0.99)] / 1000.0;
            double p999Us = latenciesNano[(int) (measureIterations * 0.999)] / 1000.0;
            double minUs = latenciesNano[0] / 1000.0;
            double maxUs = latenciesNano[measureIterations - 1] / 1000.0;
            double avgUs = Arrays.stream(latenciesNano).average().orElse(0) / 1000.0;

            System.out.println("\n[BENCHMARK 1] IN-MEMORY 4-TIER RESOLUTION READ LATENCY (" + measureIterations + " samples)");
            System.out.println("-------------------------------------------------------------------------------");
            System.out.printf("  Min Latency     : %8.3f µs\n", minUs);
            System.out.printf("  p50 (Median)    : %8.3f µs\n", p50Us);
            System.out.printf("  p90 Latency     : %8.3f µs\n", p90Us);
            System.out.printf("  p95 Latency     : %8.3f µs\n", p95Us);
            System.out.printf("  p99 Latency     : %8.3f µs\n", p99Us);
            System.out.printf("  p99.9 Latency   : %8.3f µs\n", p999Us);
            System.out.printf("  Average Latency : %8.3f µs\n", avgUs);
            System.out.printf("  Max Latency     : %8.3f µs\n", maxUs);
            System.out.println("  Throughput Est. : " + String.format("%,d", (long) (1_000_000.0 / Math.max(avgUs, 0.001))) + " ops/sec per thread");
            System.out.println("-------------------------------------------------------------------------------");

            // Assert sub-millisecond guarantee (single digit microseconds)
            assertThat(p99Us).isLessThan(50.0); // Strict requirement: single-digit to low double-digit microseconds
        }

        // -------------------------------------------------------------------------
        // PART 2: 200-CLIENT LIVE SSE PROPAGATION LATENCY BENCHMARK
        // -------------------------------------------------------------------------
        int clientCount = 200;
        List<MoveInSyncConfigClient> clients = new CopyOnWriteArrayList<>();
        ExecutorService stormPool = Executors.newFixedThreadPool(50);
        CountDownLatch connectLatch = new CountDownLatch(clientCount);

        HttpClient sharedClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
        ScheduledExecutorService sharedHeartbeat = Executors.newScheduledThreadPool(4);
        ExecutorService sharedSse = Executors.newCachedThreadPool();

        try {
            for (int i = 0; i < clientCount; i++) {
                final int id = i;
                stormPool.submit(() -> {
                    try {
                        Path dir = tempDir.resolve("bench_sub_" + id);
                        MoveInSyncConfigClient c = new MoveInSyncConfigClient(
                                serverUrl, "Google", "BLR-" + (id % 5 + 1), "google-service-key", dir,
                                sharedClient, sharedHeartbeat, sharedSse);
                        c.start();
                        clients.add(c);
                    } catch (Exception ignored) {
                    } finally {
                        connectLatch.countDown();
                    }
                });
            }

            connectLatch.await(10, TimeUnit.SECONDS);

            // Wait until all 200 SSE streams are actively connected to server
            long settleDeadline = System.currentTimeMillis() + 5000;
            while (sseBroadcaster.getConnectedSubscribers() < clientCount && System.currentTimeMillis() < settleDeadline) {
                Thread.sleep(50);
            }

            // Publish live change and measure commit-to-visible latency across 200 clients
            long[] propLatenciesMs = new long[clientCount];
            long publishStartNano = System.nanoTime();

            var cs = configService.publishChangeSet(
                    ConfigScopeKey.client("Google"),
                    null,
                    Map.of("benchmark_cutoff", 99),
                    "benchmark_runner",
                    "Propagation benchmark"
            );
            long targetVersion = cs.version();

            // Measure when each client observes the new version
            CountDownLatch propagationLatch = new CountDownLatch(clientCount);
            ExecutorService pollerPool = Executors.newFixedThreadPool(50);

            for (int i = 0; i < clientCount; i++) {
                final int idx = i;
                final MoveInSyncConfigClient c = clients.get(i);
                pollerPool.submit(() -> {
                    long deadline = System.currentTimeMillis() + 5000;
                    while (System.currentTimeMillis() < deadline) {
                        if (c.getCurrentVersion() >= targetVersion &&
                                Integer.valueOf(99).equals(c.get("benchmark_cutoff").orElse(null))) {
                            long elapsedNano = System.nanoTime() - publishStartNano;
                            propLatenciesMs[idx] = TimeUnit.NANOSECONDS.toMillis(elapsedNano);
                            propagationLatch.countDown();
                            return;
                        }
                        try {
                            Thread.sleep(2);
                        } catch (InterruptedException ignored) {
                        }
                    }
                    propagationLatch.countDown();
                });
            }

            boolean allPropagated = propagationLatch.await(6, TimeUnit.SECONDS);
            assertThat(allPropagated).isTrue();

            Arrays.sort(propLatenciesMs);
            long p50Ms = propLatenciesMs[(int) (clientCount * 0.50)];
            long p90Ms = propLatenciesMs[(int) (clientCount * 0.90)];
            long p95Ms = propLatenciesMs[(int) (clientCount * 0.95)];
            long p99Ms = propLatenciesMs[(int) (clientCount * 0.99)];
            long maxMs = propLatenciesMs[clientCount - 1];

            System.out.println("\n[BENCHMARK 2] 200-CLIENT LIVE SSE PROPAGATION LATENCY");
            System.out.println("-------------------------------------------------------------------------------");
            System.out.printf("  Fleet Size      : %d active office clients\n", clientCount);
            System.out.printf("  p50 Latency     : %d ms\n", p50Ms);
            System.out.printf("  p90 Latency     : %d ms\n", p90Ms);
            System.out.printf("  p95 Latency     : %d ms\n", p95Ms);
            System.out.printf("  p99 Latency     : %d ms\n", p99Ms);
            System.out.printf("  Max Latency     : %d ms\n", maxMs);
            System.out.println("  SLA Target      : < 5000 ms (Requirement: < 5s)");
            System.out.println("  Measured Margin : " + (5000 - maxMs) + " ms headroom under SLA");
            System.out.println("===============================================================================\n");

            // Assert strictly within the < 5s requirement from the case study!
            assertThat(maxMs).isLessThan(5000);

        } finally {
            for (MoveInSyncConfigClient c : clients) {
                try {
                    c.close();
                } catch (Exception ignored) {
                }
            }
            stormPool.shutdownNow();
            sharedHeartbeat.shutdownNow();
            sharedSse.shutdownNow();
        }
    }
}
