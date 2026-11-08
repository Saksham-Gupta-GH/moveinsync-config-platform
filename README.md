# MoveInSync Hierarchical Tenant Configuration Platform
### Distributed Configuration Engine with Sub-Microsecond In-Memory Reads & Live SSE Propagation
**Author:** Saksham Gupta  
**Email:** [saksham77779@gmail.com](mailto:saksham77779@gmail.com) | **Repository:** [github.com/Saksham-Gupta-GH/moveinsync-config-platform](https://github.com/Saksham-Gupta-GH/moveinsync-config-platform) | **Live Demo (HTTPS):** [https://20-219-130-205.sslip.io/moveinsync/](https://20-219-130-205.sslip.io/moveinsync/) | **Demo Video:** [Google Drive Walkthrough Video](https://drive.google.com/file/d/1672qzG3t7DkPtfmpotBypt1Jz9PRil3e/view?usp=sharing)

> 🌐 **Live Cloud Deployment (HTTPS with SSL):** [https://20-219-130-205.sslip.io/moveinsync/](https://20-219-130-205.sslip.io/moveinsync/)  
> *Active, public production instance deployed on Azure Cloud with verified Let's Encrypt TLS, Nginx reverse proxying, and persistent Server-Sent Events (SSE) broadcasting.*

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
|   +-----------------------+                         Local Read (~0.25–0.9 µs p50)                |
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

The figures below reflect the verified, genuine execution recorded in [docs/test-run.log](docs/test-run.log) (2 November 2026, OpenJDK 17.0.20.1 on Linux / Azure VM environment under concurrent test load). Earlier dedicated runs on idle hardware (Apple M1, macOS 14.4.1) recorded faster in-memory read latencies (~250 ns p50); the figures recorded in `test-run.log` reflect a loaded execution environment sharing CPU cycles across the reactor test suite.

### Benchmark 1: In-Memory 4-Tier Resolution Read Latency
Evaluates full 4-tier resolution (`SHIFT -> OFFICE -> CLIENT -> GLOBAL -> Default`) using `System.nanoTime()` on a single thread after 50,000 JIT warm-up iterations (100,000 measured samples under test suite load).

| Metric | Recorded result (`docs/test-run.log`) | Earlier Idle M1 Run |
|---|---|---|
| Min | 0.350 µs (350 ns) | 0.042 µs (42 ns) |
| **p50 (Median)** | **0.741 µs (741 ns)** | **0.250 µs (250 ns)** |
| p90 | 0.881 µs | 0.292 µs |
| p95 | 0.941 µs | 0.292 µs |
| p99 | 1.483 µs | 0.375 µs |
| p99.9 | 4.699 µs | 1.792 µs |
| Average | 0.947 µs | 0.280 µs |
| Maximum | 2,037.085 µs | 46.333 µs |
| Estimated throughput | 1,055,812 ops/sec per thread | ~3.6M ops/sec |

*Note: The loaded run in `docs/test-run.log` was executed on a shared cloud VM during full reactor test execution, yielding ~0.741 µs median resolution (~3× higher than an idle M1 run), both well within the sub-millisecond requirement.*

### Benchmark 2: 200-Client Live SSE Propagation Latency
Measures time from immediately before publishing (including commit and fsync) to observed visibility across **200 concurrent client instances** running within the test JVM.

| Metric | Measured Latency (`docs/test-run.log`) | Target SLA | Explanation |
|---|---|---|---|
| **Fleet Size** | **200 Active Clients** | 200 Clients | Full concurrent fleet |
| **p50 Latency** | **317 ms** | < 5,000 ms (5s) | Includes publish, fsync, and observation overhead |
| **p90 Latency** | **319 ms** | < 5,000 ms (5s) | Full concurrent test suite run |
| **p95 Latency** | **319 ms** | < 5,000 ms (5s) | Full concurrent test suite run |
| **p99 Latency** | **321 ms** | < 5,000 ms (5s) | Full concurrent test suite run |
| **Max Latency** | **321 ms** | < 5,000 ms (5s) | **4,679 ms measured headroom under SLA** |

---

## 4. Automated Test Verification Suite

The current reactor suite passes **37/37 tests** (13 core, 3 SDK, 21 server). The [latest 8 November 2026 Maven log](docs/test-run-2026-11-08-auth.txt) is the unedited output of `./mvnw test --batch-mode`; CI also uploads Surefire reports. The [earlier 8 November run](docs/test-run-2026-11-08.txt) supplies the measurements in Section 3, and the older [2 November benchmark run](docs/test-run.log) remains available.

Coverage includes hierarchy precedence and default lineage, strict type validation, multi-key atomicity, two-office canary rollout and promotion, one-version rollback across scopes with inherited-value restoration, tenant isolation, out-of-order delta recovery, 50-reader atomic snapshots, 200-client propagation, 500-client reconnects, and process-kill recovery. Rollback tests also replay earlier canary and promotion versions. These are assessment-scale tests; the measurements are not universal latency guarantees.

---

## 5. Security, RBAC & Tenant Isolation

Authentication accepts `Authorization: Bearer <key>` as described in the PDF, or the original `X-API-Key` header. Both use the same opaque credentials and RBAC rules. These are not JWTs. Conflicting headers are rejected. Browser users enter an administrator or operator key once and receive a two-hour HttpOnly, SameSite=Strict session cookie. Keys are supplied via environment variables (`MOVEINSYNC_ADMIN_KEY`, `MOVEINSYNC_OPERATOR_KEY`, `MOVEINSYNC_GOOGLE_KEY`, `MOVEINSYNC_AMAZON_KEY`); the listed demo defaults require `MOVEINSYNC_ALLOW_DEMO_KEYS=true` for local use and are refused in the `prod` profile:

| Role | Permissions | Environment Variable / Default Key |
|---|---|---|
| **`ADMIN`** | Register definitions, publish changesets, trigger rollbacks, promote rollouts | `MOVEINSYNC_ADMIN_KEY` (`admin-secret-key`) |
| **`OPERATOR`**| Publish and promote changesets across tenants (trusted platform operator) | `MOVEINSYNC_OPERATOR_KEY` (`operator-secret-key`) |
| **`EMPLOYEE`**| Read-only access to snapshots, deltas, and live SSE stream for authorized tenant | `MOVEINSYNC_GOOGLE_KEY` (`google-service-key`) |

- **Missing or Invalid Key:** Requests without a valid key receive HTTP 401 Unauthorized.
- **Tenant Isolation:** Client A cannot read Client B's overrides or deltas via REST or SSE. Error responses return generic access denial without leaking tenant names.
- **Sanitized Operational Logging:** Only operational metrics (client ID, office ID, version, latency) are logged; request bodies and employee identifiers are never logged.

---

## 6. Operational `curl` Examples

### 1. Register a Setting Definition (Admin)
```bash
curl -X POST http://localhost:8080/api/v1/config/definitions \
  -H "X-API-Key: admin-secret-key" \
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
  -H "X-API-Key: admin-secret-key" \
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
  -H "X-API-Key: admin-secret-key" \
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
  -H "X-API-Key: admin-secret-key"
```

### 5. Rollback with Dry-Run Diff Preview
```bash
# Preview the rollback diff without mutating state
curl -X POST "http://localhost:8080/api/v1/config/rollback" \
  -H "X-API-Key: admin-secret-key" -H "Content-Type: application/json" \
  -d '{"targetVersion":1,"dryRun":true,"author":"admin"}'

# Execute the rollback as a forward-reverting changeset
curl -X POST "http://localhost:8080/api/v1/config/rollback" \
  -H "X-API-Key: admin-secret-key" -H "Content-Type: application/json" \
  -d '{"targetVersion":1,"dryRun":false,"author":"admin"}'
```

### 6. Inspect Server Metrics & Lag Telemetry
```bash
curl -s -H "X-API-Key: admin-secret-key" http://localhost:8080/api/v1/metrics
```

---

## 7. Quickstart & CI

### Live Cloud Deployment (Azure)
The platform is deployed and running live on Azure Cloud VM with Let's Encrypt SSL & Nginx reverse proxying:
- **Interactive Web Dashboard (HTTPS):** [https://20-219-130-205.sslip.io/moveinsync/](https://20-219-130-205.sslip.io/moveinsync/)
- **Actuator Health Endpoint:** [https://20-219-130-205.sslip.io/moveinsync/actuator/health](https://20-219-130-205.sslip.io/moveinsync/actuator/health)
- **Live SSE Event Stream:** The signed-in dashboard connects to `/api/v1/config/stream?client_id=Google&office_id=ALL` using its browser session. Service clients can use the `X-API-Key` header.
- **Dashboard sign-in:** Enter the server's administrator or operator key. The dashboard uses a short-lived, HttpOnly browser session; no key is embedded in the page or SSE URL.
- **Production credentials:** Start the server with `SPRING_PROFILES_ACTIVE=prod` and unique `MOVEINSYNC_ADMIN_KEY`, `MOVEINSYNC_OPERATOR_KEY`, `MOVEINSYNC_GOOGLE_KEY`, and `MOVEINSYNC_AMAZON_KEY` values of at least 24 characters. The server refuses predictable demo keys in this profile. Rotate previously published keys before redeploying.

### Running the Suite Locally
```bash
# Ensure Java 17 is active
./mvnw clean test
```

### Starting the Server & Web Dashboard
```bash
./run.sh
# Server starts on port 8080 with SQLite WAL persistence
```
Once started, navigate to **`http://localhost:8080`** to access the **Interactive Policy Dashboard**:
- **4-Tier Policy Matrix:** Inspect resolved configurations across Shift, Office, Client, and Global tiers with live audit lineage (`explain`).
- **Live SSE Stream Ticker:** Watch monotonic deltas broadcast across connected subscribers in real time.
- **On-Call Telemetry:** Real-time visibility into subscriber queue depths, monotonic version sequence, and validation rejections.

### Running with Docker Compose
```bash
# Set the four MOVEINSYNC_*_KEY environment variables to unique secrets first.
docker compose up --build
```

---

## 8. Time & Space Complexity Analysis

| Operation / Path | Time Complexity | Auxiliary Space | Bottleneck / Scaling Boundary |
|---|---|---|---|
| **Local 4-Tier Read (`client.resolve`)** | $\mathcal{O}(1)$ (bounded $\le 5$ lookups) | $\mathcal{O}(1)$ | Pure in-memory hash bucket lookup; no network call or read lock. |
| **Changeset Validation (`POST /changeset`)** | $\mathcal{O}(K)$ ($K$ keys in payload) | $\mathcal{O}(K)$ | CPU-bound rule evaluation; scales linearly with changeset size. |
| **Single-Writer Commit (`SQLite WAL fsync`)** | $\mathcal{O}(K + \text{fsync})$ | $\mathcal{O}(K)$ | Disk I/O sync latency; 172 writes/sec in the recorded run. |
| **Server SSE Broadcast (`SseBroadcaster`)** | $\mathcal{O}(S \cdot K)$ ($S$ subscribers) | $\mathcal{O}(S)$ | Single-node socket/memory limits; only 500 reconnecting clients tested. |
| **Client Delta Ingestion (`ConfigSnapshot`)** | $\mathcal{O}(N + K)$ ($N$ total overrides) | $\mathcal{O}(N + K)$ | Immutable snapshot map copy; garbage collection if $N > 50\text{k}$. |

### Detailed Algorithmic Breakdown:
- **Read Path ($\mathcal{O}(1)$ Time, $\mathcal{O}(1)$ Space):** The lookup strictly follows `SHIFT -> OFFICE -> CLIENT -> GLOBAL -> Default`. Each scope tier is a direct hash map lookup (`Map<String, Map<String, Object>>`). Because hierarchy depth is fixed at 4 scopes plus 1 default lookup, execution time is constant ($\mathcal{O}(1)$) and lock-free via `AtomicReference`.
- **Write Path ($\mathcal{O}(K)$ Time, $\mathcal{O}(K)$ Space):** Validation processes $K$ keys against registered `SettingDefinition` constraints in $\mathcal{O}(K)$ time. Monotonic version increment under `ReentrantLock` writer mutex is $\mathcal{O}(1)$. Writing to SQLite WAL includes payload serialization and durable commit before returning HTTP 201.
- **Client Delta Ingestion ($\mathcal{O}(N + K)$ Time, $\mathcal{O}(S \cdot K)$ Memory):** Incoming deltas clone existing scope maps and insert new overrides in $\mathcal{O}(N + K)$ time before swapping the `AtomicReference` pointer in $\mathcal{O}(1)$ time. Client heap use grows with authorized scopes and keys; each update briefly holds both old and new snapshots.

### Where the System Stops Scaling:
1. **Write Throughput Bottleneck:** The serialized SQLite single-writer pattern caps write throughput to physical disk sync latency (172 writes/sec in the recorded run). Multi-region scaling would require sharding by tenant or migrating to distributed consensus (e.g., CockroachDB / Raft).
2. **SSE Fan-Out & File Descriptors (C10K Boundary):** In-process `ArrayBlockingQueue` per client is bounded by JVM heap and OS file descriptors (capacity beyond the 500-client test has not been measured). Beyond this, an external message broker (Kafka or Redis Streams) with stateless SSE edge nodes is needed.
3. **Client Memory Footprint at High Key Volumes:** Since client-side delta application reconstructs an unmodifiable copy of the scope map, large snapshots can increase allocation and GC pauses during frequent updates; this boundary has not been benchmarked.

---

## 9. Trade-offs & Architecture Limitations

- **Consistency and availability:** Writes commit durably under one global version before acknowledgment. Connected clients receive ordered SSE deltas and use a one-second version check to heal missed messages. During a network partition, they keep serving their last snapshot; staleness then has no finite bound. Callers capture one SDK snapshot for a consistent multi-key evaluation.
- **Rollback:** A rollback restores all override scopes in one SQLite transaction and one new version. Clients receive one tenant-filtered replacement snapshot, so inherited values are restored without exposing a mixed state. Replacing a whole tenant snapshot costs more bandwidth and allocation than an ordinary small delta.
- **Capacity:** SQLite uses one writer and the SSE broadcaster runs in one server process. The recorded commit benchmark reached 172 writes/sec on its measured machine; capacity on other hardware needs measurement. The largest reconnect test has 500 clients. Multiple server nodes would require a shared event transport and storage design.
- **Operations:** Demo credentials are for local evaluation; production deployment requires rotated, managed credentials. Browser SSE uses the session cookie, and service SSE uses an authentication header; keys in URL query strings are rejected. Metrics expose lag, rejections, errors, catch-up requests, subscriber count and queue depth. Server logs emit one JSON object per line.

See the [short design note](docs/design-note.md) for the architecture, update sequence and failure modes.

## 10. Submitted PDF

The already-submitted PDF is unchanged. Its Figure 4 was illustrative, so use the real Maven logs above for test evidence; the current code and measured results supersede its earlier estimates.
