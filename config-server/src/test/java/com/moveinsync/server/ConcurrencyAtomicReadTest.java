package com.moveinsync.server;

import com.moveinsync.client.MoveInSyncConfigClient;
import com.moveinsync.core.model.*;
import com.moveinsync.server.service.ConfigService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ConcurrencyAtomicReadTest {

    @LocalServerPort
    private int port;

    @Autowired
    private ConfigService configService;

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Concurrency Guarantee: 50 threads reading continuously never observe partial state during compound changeset publish")
    void shouldNeverSeePartialCompoundState() throws Exception {
        // Register two related settings: window start and end
        configService.registerDefinition(new SettingDefinition(
                "night_start", SettingType.NUMBER, 20, "Night start hour", ValidationRule.numberRange(0, 23)));
        configService.registerDefinition(new SettingDefinition(
                "night_end", SettingType.NUMBER, 6, "Night end hour", ValidationRule.numberRange(0, 23)));

        // Create client
        String serverUrl = "http://localhost:" + port;
        try (MoveInSyncConfigClient client = new MoveInSyncConfigClient(
                serverUrl, "Google", "BLR-1", "google-service-key", tempDir)) {
            client.start();

            int threadCount = 50;
            ExecutorService readersPool = Executors.newFixedThreadPool(threadCount);
            AtomicBoolean stop = new AtomicBoolean(false);
            AtomicInteger partialStateViolations = new AtomicInteger(0);
            AtomicInteger totalReads = new AtomicInteger(0);
            AtomicInteger readerErrors = new AtomicInteger(0);
            CountDownLatch observedFirstUpdate = new CountDownLatch(1);
            CountDownLatch observedSecondUpdate = new CountDownLatch(1);
            var firstVersion = new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE);
            var secondVersion = new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE);
            CountDownLatch startSignal = new CountDownLatch(1);

            // 50 threads reading continuously
            for (int i = 0; i < threadCount; i++) {
                readersPool.submit(() -> {
                    try {
                        startSignal.await();
                        while (!stop.get()) {
                            var snapshot = client.getSnapshot();
                            Object s = client.get(snapshot, "night_start").orElse(null);
                            Object e = client.get(snapshot, "night_end").orElse(null);

                            if (s != null && e != null) {
                                int start = ((Number) s).intValue();
                                int end = ((Number) e).intValue();

                                // Invariant: Either (20, 6) OR (22, 5) - NEVER (20, 5) or (22, 6)!
                                boolean validState1 = (start == 20 && end == 6);
                                boolean validState2 = (start == 22 && end == 5);
                                boolean validState3 = (start == 23 && end == 4);
                                if (validState2 && snapshot.version() >= firstVersion.get()) observedFirstUpdate.countDown();
                                if (validState3 && snapshot.version() >= secondVersion.get()) observedSecondUpdate.countDown();

                                if (!validState1 && !validState2 && !validState3) {
                                    partialStateViolations.incrementAndGet();
                                }
                                totalReads.incrementAndGet();
                            }
                        }
                    } catch (Exception ex) {
                        readerErrors.incrementAndGet();
                    }
                });
            }

            startSignal.countDown();

            // Publish compound changesets updating both keys simultaneously
            Thread.sleep(100);
            firstVersion.set(configService.getLatestVersion() + 1);
            configService.publishChangeSet(
                    ConfigScopeKey.client("Google"),
                    null,
                    Map.of("night_start", 22, "night_end", 5),
                    "admin",
                    "Atomic shift 1"
            );

            boolean firstObserved = observedFirstUpdate.await(5, TimeUnit.SECONDS);
            secondVersion.set(configService.getLatestVersion() + 1);
            configService.publishChangeSet(
                    ConfigScopeKey.client("Google"),
                    null,
                    Map.of("night_start", 23, "night_end", 4),
                    "admin",
                    "Atomic shift 2"
            );

            boolean secondObserved = observedSecondUpdate.await(5, TimeUnit.SECONDS);
            stop.set(true);
            readersPool.shutdown();
            readersPool.awaitTermination(3, TimeUnit.SECONDS);

            System.out.println("Total concurrent reads executed: " + totalReads.get());
            System.out.println("Partial state violations observed: " + partialStateViolations.get());

            // PROVEN GUARANTEE: In-memory atomic reference swap ensures 0 partial states
            assertThat(totalReads.get()).isGreaterThan(1000);
            assertThat(firstObserved).as("Readers must observe the first update").isTrue();
            assertThat(secondObserved).as("Readers must observe the second update").isTrue();
            assertThat(readerErrors.get()).as("Reader failures must not be swallowed").isZero();
            assertThat(partialStateViolations.get())
                    .as("No thread should ever see start from version A with end from version B")
                    .isZero();
        }
    }
}
