# MoveInSync Hierarchical Tenant Configuration Platform
### Distributed Configuration Engine with Sub-Microsecond In-Memory Reads & Live SSE Propagation
**Author:** Saksham Gupta  
**Email:** [saksham77779@gmail.com](mailto:saksham77779@gmail.com) | **Repository:** [github.com/Saksham-Gupta-GH/moveinsync-config-platform](https://github.com/Saksham-Gupta-GH/moveinsync-config-platform) | **Demo Video:** [Google Drive Walkthrough Video](https://drive.google.com/file/d/1672qzG3t7DkPtfmpotBypt1Jz9PRil3e/view?usp=sharing)

---

## 1. Overview & Problem Context
This system is a distributed, tenant-aware configuration engine built for 24/7 employee commute logistics (e.g., booking cutoffs, panic response windows, escort requirements, and fleet policies). In multi-office transportation systems, configuration access must satisfy three core requirements:

1. **Sub-Microsecond In-Memory Reads:** Routing, dispatch, and billing algorithms read configurations millions of times per hour. Reads must never hit network sockets or block behind locks.
2. **Deterministic Live Propagation (< 5 seconds):** When an operational policy changes, active offices and client instances must receive the update live without polling loops or restarts.
3. **Resilient Local Fallback:** Client SDKs must operate seamlessly during network drops, out-of-order deliveries, and cold boots from local storage.

### Technology Stack
- **Language & Runtime:** Java 17 LTS + Spring Boot 3.3.4
- **Client Cache:** Immutable snapshot pointer swap via `AtomicReference<ConfigSnapshot>`
- **Live Stream Transport:** Server-Sent Events (SSE) with HTTP chunked streaming & AWS Full Jitter backoff
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
|   +-----------------------+                         Zero-Lock Pointer Read (~166 ns)             |
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

Benchmarks were executed on an **Apple M1 (8 cores: 4 Performance + 4 Efficiency, 8 GB RAM)** running OpenJDK 17 LTS with HotSpot C2 JIT warm-up.

### Benchmark 1: In-Memory 4-Tier Resolution Read Latency
Evaluates full 4-tier resolution (`SHIFT -> OFFICE -> CLIENT -> GLOBAL -> Default`) using `System.nanoTime()` on a single thread after 50,000 JIT warm-up iterations (100,000 measured samples).

| Metric | Measured Latency | Target SLA | Performance Margin |
|---|---|---|---|
| **Min Latency** | **0.041 µs (41 ns)** | — | — |
| **p50 (Median)** | **0.166 µs (166 ns)** | < 10 µs | **60x faster** |
| **p90 Latency** | **0.208 µs (208 ns)** | < 10 µs | **48x faster** |
| **p95 Latency** | **0.209 µs (209 ns)** | < 10 µs | **47x faster** |
| **p99 Latency** | **0.250 µs (250 ns)** | < 10 µs | **40x faster** |
| **p99.9 Latency**| **0.416 µs (416 ns)** | < 50 µs | **120x faster** |
| **Throughput** | **~5.4M ops/sec** | > 100,000 ops/sec | **54x higher** |

### Benchmark 2: 200-Client Live SSE Propagation Latency
Measures end-to-end time from server SQLite commit & fsync to in-memory visibility across **200 concurrent client instances** running within the test environment.

| Metric | Measured Latency | Target SLA | Explanation |
|---|---|---|---|
| **Fleet Size** | **200 Active Clients** | 200 Clients | Full concurrent fleet |
| **p50 Latency** | **278 ms** | < 5,000 ms (5s) | Includes disk fsync + initial thread dispatch |
| **p90 Latency** | **320 ms** | < 5,000 ms (5s) | All 200 clients share single JVM thread pool |
| **p99 Latency** | **320 ms** | < 5,000 ms (5s) | Consistent event delivery across all queues |
| **Max Latency** | **320 ms** | < 5,000 ms (5s) | **4,680 ms SLA headroom** under the 5s limit |

---

## 4. Automated Test Verification Suite (26 / 26 Tests Passing)

The project includes an automated test suite across all 3 modules verifying consistency, tenant isolation, fault injection, and concurrency:

```
[INFO] Reactor Summary:
[INFO]   Config Core (Models, Validator, 4-Tier Resolution) ........ SUCCESS [11 tests]
[INFO]   Config Client SDK (Memory Cache, Disk Fallback, Gap Sync) . SUCCESS [ 2 tests]
[INFO]   Config Server (Spring Boot, SSE, Concurrency, Kill) ....... SUCCESS [13 tests]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS (Total Tests: 26, Failures: 0, Errors: 0, Skipped: 0)
```

### Complete Test Catalog:
1. `HierarchyResolverTest` (5 tests): Strict 4-tier precedence (`Shift > Office > Client > Global > Default`), audit trail `explain()` mode, cross-client override isolation.
2. `SettingValidatorTest` (6 tests): Positive duration enforcement ($\ge 0$), negative cutoff rejection ($-30\text{ min}$), time window bounds, integer/double bounds, boolean type checking.
3. `ConfigClientTest` (2 tests): Contiguous monotonic version progression, empty delta processing, cold-boot disk snapshot hydration.
4. `CompoundValidationRejectionTest`: All-or-nothing atomic validation; 1 invalid key rejects the entire compound changeset.
5. `ConcurrencyAtomicReadTest`: 50 reader threads executing 2.1M+ reads during live updates with 0 partial states observed.
6. `StagedRolloutAndPromoteTest`: Canary targeting to `BLR-1`, empty deltas to `HYD-1`, promotion endpoint expanding to all offices.
7. `RollbackDiffTest`: Dry-run diff preview without state mutation; forward-commit rollback restoring target version.
8. `TenantIsolationTest` (5 tests): Missing/unknown API key rejection (401), cross-tenant RBAC enforcement, REST snapshot/delta data isolation, and SSE tenant filtering.
9. `OutOfOrderAndGapSyncTest` (2 tests): Network reordering recovery (V43 arriving before V42 triggers delta catch-up), duplicate stale deltas ignored.
10. `ServerKillFaultInjectionTest`: Abrupt process kill via `SIGKILL (kill -9)`; committed changesets survive on reboot.
11. `ReconnectStormTest`: 200 concurrent clients reconnect simultaneously, receive live deltas, 0 dropped events.
12. `BenchmarkRunnerTest`: Empirical test validating < 10 µs read latency and < 5s live propagation SLA.

---

## 5. Security, RBAC & Tenant Isolation

Authentication uses an API key passed via the `X-API-Key` header (in production, loaded from environment variables or a secrets manager):

| Role | Permissions | Demo API Key |
|---|---|---|
| **`ADMIN`** | Register definitions, publish changesets, trigger rollbacks, promote rollouts | `admin-root-key` |
| **`OPERATOR`**| Publish changesets within authorized client tenant | `operator-key-google` |
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
    "validationRule": { "min": 0, "max": 1440 }
  }'
```

### 2. Publish a Global or Tenant Changeset (Admin / Operator)
```bash
curl -X POST http://localhost:8080/api/v1/config/publish \
  -H "X-API-Key: admin-root-key" \
  -H "Content-Type: application/json" \
  -d '{
    "scopeKey": { "type": "CLIENT", "clientId": "Google" },
    "changes": { "booking_cutoff_minutes": 30 },
    "author": "ops-lead",
    "description": "Shorten booking cutoff for Google fleet"
  }'
```

### 3. Staged Canary Rollout to Specific Office
```bash
curl -X POST http://localhost:8080/api/v1/config/publish \
  -H "X-API-Key: admin-root-key" \
  -H "Content-Type: application/json" \
  -d '{
    "scopeKey": { "type": "OFFICE", "clientId": "Google", "officeId": "BLR-1" },
    "stagedOffices": ["BLR-1"],
    "changes": { "booking_cutoff_minutes": 20 },
    "author": "ops-lead",
    "description": "Canary trial in BLR-1"
  }'
```

### 4. Promote Canary Rollout to All Offices
```bash
curl -X POST "http://localhost:8080/api/v1/config/promote?client_id=Google&target_key=booking_cutoff_minutes" \
  -H "X-API-Key: admin-root-key"
```

### 5. Rollback with Dry-Run Diff Preview
```bash
# Preview the rollback diff without mutating state
curl -X POST "http://localhost:8080/api/v1/config/rollback?target_version=1&dry_run=true" \
  -H "X-API-Key: admin-root-key"

# Execute the rollback as a forward-reverting changeset
curl -X POST "http://localhost:8080/api/v1/config/rollback?target_version=1&dry_run=false" \
  -H "X-API-Key: admin-root-key"
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
   - *Trade-off:* High write contention across thousands of independent tenants would bottleneck on a single writer. For ultra-high write scale (> 2,500 writes/sec), tenant partitioning with independent sequences would be needed.
2. **Rollback Forward-Reversion vs True State Pruning:**
   - *Design Choice:* Rollback creates a forward-reverting changeset, restoring keys to their target version values and resetting newly added keys to definition defaults.
   - *Trade-off:* Rollback never rewrites past WAL history. This guarantees client gap detectors remain intact and audit trails are preserved.
3. **SSE In-Process Broadcaster:**
   - *Capacity:* A single JVM supports 10,000–15,000 concurrent SSE subscribers before TCP file descriptor exhaustion.
   - *Multi-Node Clustering:* To scale horizontally across multiple instances, an external message broker (e.g., Redis Pub/Sub or Kafka) would fan out changesets across servers.
