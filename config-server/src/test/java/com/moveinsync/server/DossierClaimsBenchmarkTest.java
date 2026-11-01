package com.moveinsync.server;

import com.moveinsync.client.cache.ConfigSnapshot;
import com.moveinsync.core.model.ConfigScopeKey;
import com.moveinsync.core.model.SettingDefinition;
import com.moveinsync.core.model.SettingType;
import com.moveinsync.core.model.ValidationRule;
import com.moveinsync.server.service.ConfigService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures the figures the submitted dossier quotes but BenchmarkRunnerTest does not cover:
 * single-writer commit latency, 1,000-key snapshot heap footprint, and AtomicReference swap cost.
 * It only prints what it measures; it deliberately asserts no target numbers.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "spring.datasource.url=jdbc:sqlite:target/dossier_claims_bench.db")
class DossierClaimsBenchmarkTest {

    @Autowired
    private ConfigService configService;

    @BeforeAll
    static void freshDatabase() throws Exception {
        // Isolated DB so thousands of benchmark commits never affect other tests' version history
        for (String suffix : new String[]{"", "-wal", "-shm"}) {
            java.nio.file.Files.deleteIfExists(java.nio.file.Path.of("target/dossier_claims_bench.db" + suffix));
        }
    }

    @Test
    @DisplayName("Measures commit latency (WAL, synchronous=FULL), 1,000-key snapshot heap, and pointer-swap cost")
    void measureDossierClaims() {
        System.out.println("=== DOSSIER CLAIMS BENCHMARK (measured, not asserted) ===");

        // 1. Single-writer commit latency: each call is a full validated, fsync'd commit
        configService.registerDefinition(new SettingDefinition(
                "commit_bench", SettingType.NUMBER, 0, "Commit bench", ValidationRule.none()));
        int warmup = 200, n = 2_000;
        for (int i = 0; i < warmup; i++) {
            configService.publishChangeSet(ConfigScopeKey.client("BenchCo"), null,
                    Map.of("commit_bench", i), "bench", "warmup");
        }
        long[] ns = new long[n];
        long startAll = System.nanoTime();
        for (int i = 0; i < n; i++) {
            long t = System.nanoTime();
            configService.publishChangeSet(ConfigScopeKey.client("BenchCo"), null,
                    Map.of("commit_bench", i), "bench", "measure");
            ns[i] = System.nanoTime() - t;
        }
        double totalSec = (System.nanoTime() - startAll) / 1e9;
        Arrays.sort(ns);
        System.out.printf("Commit latency over %d sequential commits: p50=%.3f ms p90=%.3f ms p99=%.3f ms max=%.3f ms%n",
                n, ns[n / 2] / 1e6, ns[(int) (n * 0.90)] / 1e6, ns[(int) (n * 0.99)] / 1e6, ns[n - 1] / 1e6);
        System.out.printf("Sequential commit throughput: %.0f writes/sec%n", n / totalSec);

        // 2. Heap held by a 1,000-key snapshot (approximate: GC-settled used-heap delta)
        Runtime rt = Runtime.getRuntime();
        settleHeap();
        long before = rt.totalMemory() - rt.freeMemory();
        Map<String, SettingDefinition> defs = new HashMap<>();
        Map<String, Object> clientOverrides = new HashMap<>();
        for (int i = 0; i < 1_000; i++) {
            String key = "setting_" + i;
            defs.put(key, new SettingDefinition(key, SettingType.NUMBER, i, "Setting " + i, ValidationRule.none()));
            clientOverrides.put(key, i + 1);
        }
        Map<String, Map<String, Object>> overrides = new HashMap<>();
        overrides.put(ConfigScopeKey.client("Google").toPath(), clientOverrides);
        ConfigSnapshot snapshot = new ConfigSnapshot(1L, defs, overrides);
        settleHeap();
        long after = rt.totalMemory() - rt.freeMemory();
        System.out.printf("1,000-key snapshot heap (approx): %.2f MB (definitions + 1,000 client overrides)%n",
                Math.max(0, after - before) / 1048576.0);
        assertThat(snapshot.version()).isEqualTo(1L); // keeps the snapshot reachable during measurement

        // 3. AtomicReference.set cost (microbenchmark; excludes building the new snapshot)
        AtomicReference<ConfigSnapshot> ref = new AtomicReference<>(snapshot);
        ConfigSnapshot next = snapshot.withVersion(2L);
        for (int i = 0; i < 5_000_000; i++) ref.set((i & 1) == 0 ? next : snapshot);
        int swaps = 20_000_000;
        long t0 = System.nanoTime();
        for (int i = 0; i < swaps; i++) ref.set((i & 1) == 0 ? next : snapshot);
        System.out.printf("AtomicReference.set average: %.2f ns/op (loop overhead included)%n",
                (System.nanoTime() - t0) / (double) swaps);
        System.out.println("=== END DOSSIER CLAIMS BENCHMARK ===");
    }

    private static void settleHeap() {
        for (int i = 0; i < 3; i++) {
            System.gc();
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }
}

