# MoveInSync Hierarchical Tenant Configuration Platform
### Advanced SDE Backend Track — Case Study 2: Live Configuration Propagation System
**Author:** Saksham Gupta (Reg No: `230911186` | MIT Manipal, Top 5%, CGPA: 9.12)  
**Email:** [saksham77779@gmail.com](mailto:saksham77779@gmail.com) | **Repository:** [github.com/Saksham-Gupta-GH/moveinsync-config-platform](https://github.com/Saksham-Gupta-GH/moveinsync-config-platform)

---

## 1. Executive Summary & Design Philosophy
This system is an enterprise-grade, high-throughput distributed configuration engine engineered for MoveInSync's mission-critical employee commute logistics platform. In 24/7 transport operations (e.g. shift cutoffs, escort allocations, panic windows), configuration access must satisfy three non-negotiable guarantees:
1. **Sub-Microsecond In-Memory Reads:** Commute routing decisions cannot block on network I/O or synchronized database locks.
2. **Deterministic Live Propagation (< 5 seconds):** Operational policy updates broadcast live to all active offices and vehicle tablets.
3. **Rock-Solid Fault Tolerance:** Zero partial states, atomic crash recovery, network gap healing, and resilient cold-boot survival.

### Technology Stack
- **Runtime & Language:** Java 17 LTS + Spring Boot 3.3.4 (Strictly aligned with MoveInSync's production backend).
- **Client Architecture:** Zero-lock immutable snapshot swap via `AtomicReference<ConfigSnapshot>`.
- **Live Stream Transport:** Server-Sent Events (SSE) with HTTP/1.1 chunked streaming & AWS Full Jitter backoff.
- **Storage & Durability:** Embedded SQLite in WAL mode with `PRAGMA synchronous = FULL;` (Fsync before write-acknowledgment).
- **Packaging:** Multi-module Maven (`config-core`, `config-client-sdk`, `config-server`), Docker, and Docker Compose.

---

## 2. Architecture & Core Mechanisms

```
                     +---------------------------------------------+
                     |         Central Configuration Server        |
                     |  - REST Control Plane (Admin / Operator)    |
                     |  - Multi-Key Atomic Validator Engine        |
                     |  - Single Writer ReentrantLock              |
                     |  - SQLite WAL (synchronous = FULL)          |
                     |  - SSE Broadcaster (Bounded Queues: cap 100)|
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
|   +-----------------------+                         Zero-Lock Pointer Read: 166 ns               |
|   | Gap Detector Engine   |                                            |                         |
|   | (v > current + 1)     |                                            v                         |
|   +-----------------------+                      +-------------------------------------------+   |
|              | (Triggers delta sync)             | 4-Tier Hierarchical Resolution Engine     |   |
|              v                                   | SHIFT -> OFFICE -> CLIENT -> GLOBAL -> Def|   |
|   +-----------------------+                      +-------------------------------------------+   |
|   | Atomic Disk Fallback  |                                            |                         |
|   | (snapshot.json atomic)| <------------------------------------------+                         |
|   +-----------------------+                                                                      |
+--------------------------------------------------------------------------------------------------+
```

### Key Architectural Solutions:

#### 1. Zero-Lock Atomic Read Isolation
The Client SDK stores configuration state in an unmodifiable `ConfigSnapshot` record referenced by an `AtomicReference<ConfigSnapshot>`.
- Multi-key reads acquire the local snapshot reference once (`client.getSnapshot()`), guaranteeing **100% snapshot isolation** across compound keys with **zero synchronized locks** and **zero thread contention**.
- **Verified Concurrency:** 50 threads executing **900,000+ continuous concurrent reads** during live compound updates observed **0 partial states**.

#### 2. Staged Canary Rollout Without False Gap Alarms (Option A)
When a changeset targets specific canary offices (e.g. `BLR-1`), other offices (`HYD-1`) must not receive a breaking version gap that triggers unnecessary REST polling storms.
- **Solution:** Out-of-scope offices receive a **contiguous empty delta** carrying the same incremented version number (`empty = true`).
- Their local version counter advances contiguously (`V43 -> V44 -> V45`), preventing gap alarms while ensuring canary-isolated data isolation.

#### 3. Guaranteed Crash Durability (Surviving SIGKILL `kill -9`)
Many embedded systems lose the most recent commit on power loss due to un-flushed OS disk buffers.
- **Solution:** SQLite is configured with `PRAGMA journal_mode = WAL;` and `PRAGMA synchronous = FULL;`. Every committed changeset is explicitly fsynced to persistent storage *before* the HTTP 201 response is returned.
- **Verified via Fault Injection:** A live external process killed forcibly via OS `SIGKILL` restarts against the database with 100% data intact.

#### 4. Slow Consumer Isolation & Bounded Queues
- Each SSE connection maintains an `ArrayBlockingQueue<Delta>(100)`. If a slow client fails to consume events and overflows the buffer, it is evicted immediately (`completeWithError`), protecting server heap from memory exhaustion and forcing that client to reconnect and perform clean delta resync.

---

## 3. Measured Performance Benchmarks

All benchmarks were executed on an Apple M-series ARM64 architecture with **HotSpot C2 JIT warm-up (50,000 iterations)** to measure steady-state production performance.

### Benchmark 1: In-Memory 4-Tier Resolution Read Latency (100,000 Samples)
Evaluates full hierarchical path resolution (`SHIFT -> OFFICE -> CLIENT -> GLOBAL -> Default`).

| Metric | Measured Latency | Evaluator SLA Requirement | Performance Margin |
|---|---|---|---|
| **Min Latency** | **0.041 µs (41 ns)** | — | — |
| **p50 (Median)** | **0.166 µs (166 ns)** | < 10 µs | **60x faster** |
| **p90 Latency** | **0.208 µs (208 ns)** | < 10 µs | **48x faster** |
| **p95 Latency** | **0.209 µs (209 ns)** | < 10 µs | **47x faster** |
| **p99 Latency** | **0.250 µs (250 ns)** | < 10 µs | **40x faster** |
| **p99.9 Latency**| **0.416 µs (416 ns)** | < 50 µs | **120x faster** |
| **Average** | **0.184 µs (184 ns)** | — | — |
| **Single-Thread Throughput** | **5,431,644 ops/sec** | > 100,000 ops/sec | **54x higher** |

### Benchmark 2: 200-Client Live SSE Propagation Latency
Measures end-to-end time from server SQLite commit & fsync to in-memory visibility across **200 concurrent office clients**.

| Metric | Measured Latency | Evaluator SLA Requirement | Performance Margin |
|---|---|---|---|
| **Fleet Size** | **200 Active Clients** | 200 Clients | Complete fleet coverage |
| **p50 Latency** | **278 ms** | < 5,000 ms (5s) | **18x faster** |
| **p90 Latency** | **320 ms** | < 5,000 ms (5s) | **15x faster** |
| **p95 Latency** | **320 ms** | < 5,000 ms (5s) | **15x faster** |
| **p99 Latency** | **320 ms** | < 5,000 ms (5s) | **15x faster** |
| **Max Latency** | **320 ms** | < 5,000 ms (5s) | **4,680 ms SLA Headroom** |

---

## 4. Test Verification Suite (24 / 24 Tests Passing)

The project includes an exhaustive, multi-tier automated test suite covering all failure modes, edge cases, and hard rules:

```
[INFO] Reactor Summary:
[INFO]   Config Core (Models, Validator, 4-Tier Resolution) .... SUCCESS [11 tests]
[INFO]   Config Client SDK (Memory Cache, Disk Fallback, Gap) .. SUCCESS [ 2 tests]
[INFO]   Config Server (Spring Boot, SSE, Concurrency, Kill) ... SUCCESS [11 tests]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS (Total Tests: 24, Failures: 0, Errors: 0, Skipped: 0)
```

### Complete Test Catalog:
1. `HierarchyResolverTest` (5 tests):
   - Strict 4-tier resolution order (`Shift > Office > Client > Global > Default`).
   - Audit trail `explain()` mode returning exact override source and evaluation path.
   - Cross-client override isolation.
2. `SettingValidatorTest` (6 tests):
   - Positive duration enforcement ($\ge 0$). Rejection of negative cutoffs ($-30\text{ min}$).
   - Time window validation (Start hour $\ne$ End hour).
   - Bounds validation for integers, doubles, and boolean flag typing.
3. `ConfigClientTest` (2 tests):
   - Contiguous monotonic version progression and empty delta handling.
   - Cold-boot disk snapshot hydration and atomic file swaps.
4. `CompoundValidationRejectionTest`:
   - All-or-nothing atomic validation: If 1 key in a compound changeset is invalid, the entire changeset is rejected; zero keys publish and version does not advance.
5. `ConcurrencyAtomicReadTest`:
   - 50 reader threads executing 900,000+ reads simultaneously during active compound publishes; **0 partial state violations**.
6. `StagedRolloutAndPromoteTest`:
   - Canary targeting to `BLR-1`; non-canary `HYD-1` receives contiguous empty deltas; promotion endpoint `/promote` safely expands to all offices.
7. `RollbackDiffTest`:
   - Dry-run preview returning exact JSON diff without mutating state. Forward-commit rollback reverting configuration safely.
8. `TenantIsolationTest`:
   - RBAC enforcement (`ADMIN`, `OPERATOR`, `EMPLOYEE`).
   - Tenant isolation preventing Client A from reading or receiving Client B's configurations via REST or SSE.
9. `OutOfOrderAndGapSyncTest`:
   - Network reordering simulation: Delivering V43 to client at V41 triggers gap detector, queries `/api/v1/config/deltas?since_version=41`, and restores monotonic sequence.
   - Duplicate stale deltas (e.g. V41 delivered again) silently ignored as no-ops.
10. `ServerKillFaultInjectionTest`:
    - Hard process failure: Launches server via `ProcessBuilder`, commits changeset, executes `SIGKILL (kill -9)`, and verifies change survives on restarted server.
11. `ReconnectStormTest`:
    - 200 simulated clients reconnect concurrently, receive live SSE deltas, and maintain 0 queue drop evictions.
12. `BenchmarkRunnerTest`:
    - Automated JIT-warmed benchmark runner validating microsecond reads and < 5s propagation.

---

## 5. Security, RBAC & Tenant Isolation

| Role | Permissions | API Key Sample |
|---|---|---|
| **`ADMIN`** | Register definitions, publish changesets, trigger rollbacks, promote rollouts | `admin-root-key` |
| **`OPERATOR`**| Publish operational changesets within authorized client tenant | `operator-key-google` |
| **`EMPLOYEE`**| Read-only access to snapshots, deltas, and live SSE stream for their tenant | `google-service-key` |

- **Zero Information Leakage:** Production error responses emit standardized error codes (`VALIDATION_ERROR`, `TENANT_ACCESS_DENIED`, `NOT_FOUND`) without leaking internal database stack traces.
- **PII Protection Policy:** The server and client strictly log only request metadata (client ID, office ID, version number, timing) and **never log request payloads or employee identifiers**.

---

## 6. One-Command Quickstart & Deployment

### Prerequisites
- JDK 17+ installed (or Docker).

### Option 1: Native One-Command Startup
```bash
# Clone the repository
git clone https://github.com/Saksham-Gupta-GH/moveinsync-config-platform.git
cd moveinsync-config-platform

# Run the complete test suite and start the server
./run.sh
```

### Option 2: Docker Compose Startup
```bash
docker compose up --build
```

### Verifying Endpoints
- **Health Check:** `curl -s http://localhost:8080/actuator/health`
- **Prometheus Metrics:** `curl -s http://localhost:8080/api/v1/metrics`
- **Fetch Snapshot:**
  ```bash
  curl -s -H "X-API-Key: google-service-key" \
    "http://localhost:8080/api/v1/config/snapshot?client_id=Google&office_id=BLR-1"
  ```
- **Stream Live Changes (SSE):**
  ```bash
  curl -N -H "X-API-Key: google-service-key" \
    "http://localhost:8080/api/v1/config/stream?client_id=Google&office_id=BLR-1"
  ```

---

## 7. Trade-offs & Scalability Limits

1. **Storage Tier (SQLite WAL vs Distributed RDBMS):**
   - *Choice:* SQLite with WAL mode was chosen for this single-node case study because it offers single-process speed, zero external dependencies, and atomic fsync durability.
   - *Scaling Limit:* SQLite utilizes a single-writer lock. For write throughput beyond ~2,500 changesets/second, the storage layer should migrate to PostgreSQL with read replicas.
2. **SSE Streaming Tier (Single Node vs Distributed Fleet):**
   - *Single Node Capacity:* With non-blocking chunked streaming, a single JVM instance comfortably supports 10,000–15,000 concurrent SSE subscribers before TCP socket file-descriptor limits occur.
   - *Horizontal Scaling:* For fleets exceeding 50,000 vehicle devices, cluster nodes can coordinate delta broadcasts via Redis Pub/Sub or Apache Kafka with consistent hashing per tenant.
