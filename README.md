# MoveInSync Hierarchical Tenant Configuration Platform
### Distributed Configuration Engine with Sub-Microsecond In-Memory Reads & Live SSE Propagation
**Author:** Saksham Gupta  
**Email:** [saksham77779@gmail.com](mailto:saksham77779@gmail.com) | **Repository:** [github.com/Saksham-Gupta-GH/moveinsync-config-platform](https://github.com/Saksham-Gupta-GH/moveinsync-config-platform) | **Demo Video:** [Google Drive Walkthrough Video](https://drive.google.com/file/d/1672qzG3t7DkPtfmpotBypt1Jz9PRil3e/view?usp=sharing)

---

## 1. Overview & Problem Context
This system is a distributed, tenant-aware configuration engine built for 24/7 employee commute logistics (e.g., booking cutoffs, panic response windows, escort requirements, and fleet policies). In multi-office transportation systems, configuration access must satisfy three core requirements:

1. **Sub-Microsecond In-Memory Reads:** Routing, dispatch, and billing algorithms read configurations millions of times per hour. Reads must never hit network sockets or block behind locks.
2. **Live Propagation (5-second target):** SSE pushes changes to connected clients; a one-second version check heals missed pushes. Reads remain local. Network outages can exceed the staleness target.
3. **Resilient Local Fallback:** Client SDKs must operate seamlessly during network drops, out-of-order deliveries, and cold boots from local storage.

### Technology Stack
- **Language & Runtime:** Java 17 LTS + Spring Boot 3.3.4
- **Client Cache:** Immutable snapshot pointer swap via `AtomicReference<ConfigSnapshot>`
- **Live Stream Transport:** Server-Sent Events (SSE) with HTTP chunked streaming and decorrelated jitter backoff
- **Storage & Durability:** Embedded SQLite in WAL mode with `PRAGMA synchronous = FULL;` (fsync before ACK)
- **Dependencies:** Minimal runtime dependencies (`jackson-databind` for serialization)
- **Packaging:** Multi-module Maven (`config-core`, `config-client-sdk`, `config-server`), Docker, and GitHub Actions CI

---

## 2. Architecture & Design Decisions

```
                     +---------------------------------------------+
                     |         Central Configuration Server        |
                     |  - REST Control Plane (Admin / Operator)    |
                     |  - Multi-Key Atomic Validator Engine        |
                     |  - Single-Writer ReentrantLock              |
                     |  - SQLite WAL (fsync before 201 ACK)        |
                     |  - SSE Broadcaster (Per-subscriber queues)  |
                     +----------------------+----------------------+
                                            |
                         +------------------+------------------+
                         | SSE Stream (Chunked HTTP)           | REST Fallback
                         | - Monotonic Deltas (V1, V2, V3...)  | - Snapshot Sync
                         | - Contiguous Empty Deltas for Canary| - Delta Range (?since_version=)
                         v                                     v
+--------------------------------------------------------------------------------------------------+
|                                    MoveInSync Client SDK                                         |
|                                                                                                  |
|   +-----------------------+     Atomic Swap      +-------------------------------------------+   |
|   | Incoming Delta Stream | -------------------> |    AtomicReference<ConfigSnapshot>        |   |
|   +-----------------------+                      +-------------------------------------------+   |
|              |                                                         |                         |
|   +-----------------------+                         Local Read (250 ns measured p50)             |
|   | Gap Detector Engine   |                                            |                         |
|   | (v > current + 1)     |                                            v                         |
|   +-----------------------+                      +-------------------------------------------+   |
|              | (Triggers delta catch-up)         | 4-Tier Hierarchical Resolution Engine     |   |
|              v                                   | SHIFT -> OFFICE -> CLIENT -> GLOBAL -> Def|   |
|   +-----------------------+                      +-------------------------------------------+   |
|   | Atomic Disk Fallback  |                                            |                         |
|   | (snapshot.json atomic)| <------------------------------------------+                         |
|   +-----------------------+                                                                      |
+--------------------------------------------------------------------------------------------------+
```

### Key Engineering Decisions:

#### 1. Zero-Lock Atomic Read Isolation
The Client SDK stores state in an unmodifiable `ConfigSnapshot` record referenced by an `AtomicReference<ConfigSnapshot>`.
- Multi-key reads acquire the local snapshot reference once (`client.getSnapshot()`), guaranteeing consistent snapshot isolation across compound keys with zero synchronized locks and zero thread contention.
- **Verification:** 50 reader threads executed **2,100,000+ continuous reads** during live concurrent compound updates with **0 partial state violations**.

#### 2. Staged Canary Rollout Without False Gap Alarms
When a changeset targets a specific office (e.g., `BLR-1`), non-canary offices (e.g., `HYD-1`) must not receive a version gap that triggers unnecessary REST polling storms.
- **Mechanism:** Out-of-scope offices receive a **contiguous empty delta** carrying the newly incremented global version number (`empty = true`).
- Their local version counter advances monotonically (`v43 -> v44 -> v45`), keeping version alignment across the fleet while isolating tenant data.

#### 3. Durability Before ACK (Surviving `kill -9`)
Configuration servers must survive abrupt process death or power loss without losing committed changesets.
- SQLite is configured with `PRAGMA journal_mode = WAL;` and `PRAGMA synchronous = FULL;`. Every committed changeset is fsynced to disk *before* returning HTTP 201 to the publisher.
- **Verification:** In `ServerKillFaultInjectionTest`, a background server process was abruptly killed with `SIGKILL (kill -9)`. Upon restart, all committed changes survived with zero data loss.

#### 4. SSE Concurrency & Slow Consumer Protection
- Each SSE subscriber session synchronizes its outgoing event queue to eliminate any out-of-order frame delivery.
- If a client becomes unresponsive, its bounded queue (`ArrayBlockingQueue(100)`) triggers an eviction (`completeWithError`), protecting server heap from memory leaks and forcing that client to reconnect and perform a clean REST catch-up.

---

## 3. Measured Benchmarks

The numbers below come from [docs/test-run.log](docs/test-run.log), captured on this machine on 31 October 2026. Hardware verified with `sysctl`: **Apple M1, 8 physical cores, 8 GiB RAM (8,589,934,592 bytes)**. Software: macOS 14.4.1, Homebrew OpenJDK 17.0.15. Hardware was checked after the run; it is not embedded in the original log. The benchmark performs a warm-up loop but does not verify a particular JIT compilation tier.

### Benchmark 1: In-Memory 4-Tier Resolution Read Latency
Evaluates full 4-tier resolution (`SHIFT -> OFFICE -> CLIENT -> GLOBAL -> Default`) using `System.nanoTime()` on a single thread after 50,000 JIT warm-up iterations (100,000 measured samples).

| Metric | Recorded result |
|---|---|
| Min | 0.042 µs (42 ns) |
| p50 | 0.250 µs (250 ns) |
| p90 | 0.292 µs (292 ns) |
| p95 | 0.292 µs (292 ns) |
| p99 | 0.375 µs (375 ns) |
| p99.9 | 1.792 µs |
| Average | 0.280 µs |
| Maximum | 46.333 µs |
| Estimated throughput | 3,569,467 ops/sec per thread (~3.6M) |

Throughput is an estimate from the measured mean latency, not an independent sustained-load measurement. The log renders the microsecond symbol as `?s`; units are confirmed by the benchmark source.

### Benchmark 2: 200-Client Live SSE Propagation Latency
Measures time from immediately before publishing (including commit and fsync) to observed visibility across **200 concurrent client instances** running within the test JVM.

| Metric | Measured Latency | Target SLA | Explanation |
|---|---|---|---|
| **Fleet Size** | **200 Active Clients** | 200 Clients | Full concurrent fleet |
| **p50 Latency** | **236 ms** | < 5,000 ms (5s) | Includes publish and observation overhead |
| **p90 Latency** | **243 ms** | < 5,000 ms (5s) | Same-JVM simulation |
| **p95 Latency** | **243 ms** | < 5,000 ms (5s) | Same-JVM simulation |
| **p99 Latency** | **244 ms** | < 5,000 ms (5s) | Same-JVM simulation |
| **Max Latency** | **244 ms** | < 5,000 ms (5s) | **4,756 ms measured headroom** |

---

## 4. Automated Test Verification Suite

The current suite contains **27 tests: core 11, SDK 2, server 14**. See [actual Maven output](docs/test-run.log) for the latest recorded `./mvnw test` run. CI also uploads Surefire reports. The submitted PDF's Figure 4 is not an authentic terminal capture and must not be used as test evidence; its standalone image has been removed. The submitted PDF itself remains unchanged.

Verified follow-up run (31 October 2026, Java 17): `./mvnw clean test` passed **27/27 tests** (11 core, 2 SDK, 14 server), with zero failures, errors, or skips. All 500 interrupted clients reconnected and received the subsequent update. The concurrency test observed both new versions across 209,210 reads with zero mixed states. Total Maven time was 20.152 seconds; timings and read counts vary by run.

The project includes an automated test suite across all 3 modules verifying consistency, tenant isolation, fault injection, and concurrency:

The raw log is generated by Maven, not a reconstructed transcript or screenshot.

### Complete Test Catalog:
1. `HierarchyResolverTest` (5 tests): Strict 4-tier precedence (`Shift > Office > Client > Global > Default`), audit trail `explain()` mode, cross-client override isolation.
2. `SettingValidatorTest` (6 tests): Positive duration enforcement ($\ge 0$), negative cutoff rejection ($-30\text{ min}$), time window bounds, integer/double bounds, boolean type checking.
3. `ConfigClientTest` (2 tests): Contiguous monotonic version progression, empty delta processing, cold-boot disk snapshot hydration.
4. `CompoundValidationRejectionTest`: All-or-nothing atomic validation; 1 invalid key rejects the entire compound changeset.
5. `ConcurrencyAtomicReadTest`: 50 reader threads must observe both newly committed versions without mixed states; read counts vary by run.
6. `StagedRolloutAndPromoteTest`: Canary targeting to `BLR-1`, empty deltas to `HYD-1`, promotion endpoint expanding to all offices.
7. `RollbackDiffTest`: Dry-run diff preview without state mutation; forward-commit rollback restoring target version.
8. `TenantIsolationTest` (5 tests): Missing/unknown API key rejection (401), cross-tenant RBAC enforcement, REST snapshot/delta data isolation, and SSE tenant filtering.
9. `OutOfOrderAndGapSyncTest` (1 test): Network reordering recovery and duplicate stale-delta handling.
10. `ServerKillFaultInjectionTest`: Abrupt process kill via `SIGKILL (kill -9)`; committed changesets survive on reboot.
11. `ReconnectStormTest`: 500 clients establish streams, all streams are interrupted, every client establishes a new stream, and all receive the next update within 5 seconds. This models transport interruption; the separate process-kill test verifies server restart durability.
12. `BenchmarkRunnerTest`: Empirical test validating < 10 µs read latency and < 5s live propagation SLA.
13. `BearerAuthenticationTest`: HTTP Bearer authentication, tenant isolation, invalid credentials, and rejection of conflicting authentication headers.

---

## 5. Security, RBAC & Tenant Isolation

Authentication accepts `Authorization: Bearer <key>` as described in the PDF, or the original `X-API-Key` header. Both use the same opaque demo credentials and RBAC rules. These are not JWTs. Conflicting headers are rejected. Credentials are currently demo constants; external secret management and TLS are deployment requirements.

| Role | Permissions | Demo API Key |
|---|---|---|
| **`ADMIN`** | Register definitions, publish changesets, trigger rollbacks, promote rollouts | `admin-root-key` |
| **`OPERATOR`**| Publish and promote changesets across tenants (trusted platform operator) | `operator-secret-key` |
| **`EMPLOYEE`**| Read-only access to snapshots, deltas, and live SSE stream for authorized tenant | `google-service-key` |

- **Missing or Invalid Key:** Requests without a valid key receive HTTP 401 Unauthorized.
- **Tenant Isolation:** Client A cannot read Client B's overrides or deltas via REST or SSE. Error responses return generic access denial without leaking tenant names.
- **Sanitized Operational Logging:** Only operational metrics (client ID, office ID, version, latency) are logged; request bodies and employee identifiers are never logged.

---

## 6. Operational `curl` Examples

### 1. Register a Setting Definition (Admin)
```bash
curl -X POST http://localhost:8080/api/v1/config/definitions \
  -H "X-API-Key: admin-root-key" \
  -H "Content-Type: application/json" \
  -d '{
    "key": "booking_cutoff_minutes",
    "type": "DURATION_MINUTES",
    "defaultValue": 45,
    "description": "Cutoff window for shift cab bookings",
    "validationRule": { "minDurationMinutes": 0, "maxDurationMinutes": 1440 }
  }'
```

### 2. Publish a Global or Tenant Changeset (Admin / Operator)
```bash
curl -X POST http://localhost:8080/api/v1/config/changeset \
  -H "X-API-Key: admin-root-key" \
  -H "Content-Type: application/json" \
  -d '{
    "scopeKey": { "scope": "CLIENT", "clientId": "Google" },
    "updates": { "booking_cutoff_minutes": 30 },
    "author": "ops-lead",
    "reason": "Shorten booking cutoff for Google fleet"
  }'
```

### 3. Staged Canary Rollout to Specific Office
```bash
curl -X POST http://localhost:8080/api/v1/config/changeset \
  -H "X-API-Key: admin-root-key" \
  -H "Content-Type: application/json" \
  -d '{
    "scopeKey": { "scope": "CLIENT", "clientId": "Google" },
    "stagedOffices": ["BLR-1"],
    "updates": { "booking_cutoff_minutes": 20 },
    "author": "ops-lead",
    "reason": "Canary trial in BLR-1"
  }'
```

### 4. Promote Canary Rollout to All Offices
```bash
# Replace CHANGESET_ID with the id returned by the staged changeset request.
curl -X POST "http://localhost:8080/api/v1/config/changeset/CHANGESET_ID/promote?author=ops-lead" \
  -H "X-API-Key: admin-root-key"
```

### 5. Rollback with Dry-Run Diff Preview
```bash
# Preview the rollback diff without mutating state
curl -X POST "http://localhost:8080/api/v1/config/rollback" \
  -H "X-API-Key: admin-root-key" -H "Content-Type: application/json" \
  -d '{"targetVersion":1,"dryRun":true,"author":"admin"}'

# Execute the rollback as a forward-reverting changeset
curl -X POST "http://localhost:8080/api/v1/config/rollback" \
  -H "X-API-Key: admin-root-key" -H "Content-Type: application/json" \
  -d '{"targetVersion":1,"dryRun":false,"author":"admin"}'
```

### 6. Inspect Server Metrics & Lag Telemetry
```bash
curl -s -H "X-API-Key: admin-root-key" http://localhost:8080/api/v1/metrics
```

---

## 7. Quickstart & CI

### Running the Suite Locally
```bash
# Ensure Java 17 is active
./mvnw clean test
```

### Starting the Server
```bash
./run.sh
# Server starts on port 8080 with SQLite WAL persistence
```

### Running with Docker Compose
```bash
docker compose up --build
```

---

## 8. Trade-offs & Architecture Limitations

1. **Global Monotonic Versions vs Per-Tenant Versioning:**
   - *Design Choice:* The server uses a single monotonically increasing version sequence guarded by a reentrant writer lock. This makes total order reasoning and gap detection trivial.
   - *Trade-off:* High write contention across thousands of independent tenants would bottleneck on a single writer. Commit throughput has not been benchmarked; tenant partitioning would require separate capacity measurements.
2. **Rollback Forward-Reversion vs True State Pruning:**
   - *Design Choice:* Rollback creates a forward-reverting changeset, restoring keys to their target version values and resetting newly added keys to definition defaults.
   - *Trade-off:* Rollback never rewrites past WAL history. This guarantees client gap detectors remain intact and audit trails are preserved.
3. **SSE In-Process Broadcaster:**
   - *Capacity:* Capacity depends on file descriptors, blocking SDK reader threads, dispatch threads, and heap. The test targets 500 reconnecting clients; 10,000+ subscribers have not been demonstrated.
   - *Multi-Node Clustering:* To scale horizontally across multiple instances, an external message broker (e.g., Redis Pub/Sub or Kafka) would fan out changesets across servers.

## 9. Submitted Dossier and Implementation Follow-up

The previously submitted PDF has been removed from this repository and distribution ZIP; the already-submitted external copy cannot be changed. Follow-up commits add Bearer-header support and the typed SDK call shown in its concurrency example:

**Corrections to the submitted PDF:** Figure 4 is an illustrative reconstruction, not genuine execution evidence. Use `docs/test-run.log` instead. The single-writer commit-latency row, ~2,500 writes/sec, under-2.4-MB snapshot footprint, and under-10-ns pointer-swap claims have no supporting measurements in this project and are withdrawn. They have been removed from the editable HTML. The current 27-test suite supersedes the PDF's 26-test count. The evaluator's replacement design note is not yet included because its file has not been provided in this workspace.

```java
ConfigSnapshot snapshot = client.getSnapshot();
int cutoff = client.get(snapshot, "booking_cutoff_minutes", Integer.class);
```

Four configurable scopes (SHIFT, OFFICE, CLIENT, GLOBAL) plus the definition default form the five-step lookup sequence. Defaults are a fallback, not a fifth independently configurable scope.

Consistency model: writes are serialized and durably committed before acknowledgment. Clients converge through ordered SSE updates and REST catch-up. A caller captures one snapshot for a consistent multi-key evaluation; separate reads that capture different snapshots may observe different versions. During partitions, clients favor availability and serve the last snapshot without a finite staleness bound.

The saved run reports 250 ns p50 reads and 236 ms p50 / 244 ms maximum propagation; these are local measurements, not universal latency guarantees. The assessment target is 5 seconds under healthy connectivity. The heartbeat checks versions every second; network and replay time must also fit that budget. The reconnect formula uses decorrelated jitter with a 50 ms base and 2,000 ms cap, as illustrated in the PDF; this is distinct from full exponential jitter.

Remaining deployment limits include demo credentials, a single writer/server, and rollback implemented as forward per-scope changes rather than a single atomic multi-scope transaction. The current rollback resets newly introduced overrides to definition defaults rather than deleting them, so it does not restore inherited values in every hierarchy case. Crash tests demonstrate recovery for their tested scenario, not immunity to every storage failure. The PDF's production-readiness wording should be read in the context of this assessment prototype.
