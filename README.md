# CS324 A1 — Distributed Computing Cluster (Bootstrap / Workers / Election)

## Project Overview

This project is a distributed computing cluster built with **Java RMI**. These are **six independent worker processes** connected in an unstructured network, a neutral
**Bootstrap Node** tracking membership, and a distributed **leader election** that lets the
workers agree on a **Coordinator** for one term at a time. Clients submit jobs (MAX, PRIMECOUNT,
PRIMESUM); the coordinator splits the work evenly across all reachable active workers, RANKING the
workers by **JAC** (lowest first) so the least-loaded nodes get work first, merges the partial
results and returns the final answer.

Each worker runs in its own JVM with its own RMI registry port, its own log file and its own
pid file. Workers only know their direct neighbours (a ring backbone built from the Bootstrap
registry plus random connections so new workers are randomly linked to an active worker), but
elections and job distribution reason over **all reachable active workers** by forwarding messages
hop-by-hop over the neighbour graph.
---

## Assignment Requirements

### Non-Functional Requirements

- **Clear separation of components** — `api` (contracts), `bootstrap`, `worker`, `frontend.client`.
- **Thread safety** — `AtomicInteger` for JAC / `jobsThisTerm` / coordinator id; concurrent key
  sets for neighbours and processed election ids; terminal term hand-over via `compareAndSet`.
- **Failure handling** — unreachable neighbours are skipped; RMI exceptions are wrapped and
  surfaced to the client; worker shutdown unregisters from the Bootstrap Node.
- **No hard-coded network addresses (where avoidable)** — host and ports are CLI/field
  configurable; only the fixed worker layout (IDs/ports) is centralised in `WorkerClusterConfig`.
- **Meaningful logging** — each worker logs in `logs/worker-<id>.log`; the GUIs log to a panel.
- **Modular object-oriented design** — RMI interfaces + DTOs in `com.cs324.backend.api`.
- **Correct exception handling** — input validation on client and worker; over-term submissions
  are refused with a clear message.

### Component Responsibilities

- **Bootstrap Node** — only tracks active workers; it never runs jobs and never participates in
  elections. Exposes `registerWorker`, `unregisterWorker`, `getActiveWorkers`, `getRandomWorker`.
- **Worker** — registers with the Bootstrap Node, maintains neighbours, participates in elections
  and runs distributed computations. Tracks a lifetime **JAC** (jobs allocated) and a per-term
  submitted-job counter.
- **Coordinator** — a worker elected for one term. Receives jobs from clients, finds reachable
  active workers, divides work evenly, sends sub-jobs, combines partial results, assigns at most
  **five jobs per term**, then steps down and triggers the next election.
- **Client** — a separate process with a GUI that accepts manual or CSV input and submits jobs
  concurrently.

---

## Election & Tie-Breaking Rules

1. Any worker can initiate an election while no coordinator is present.
2. Every worker runs a small background check (staggered per worker) and quietly starts an
   election when the cluster has no coordinator. This is automatic: a freshly started cluster
   elects by itself, and a cluster whose 5-job term ends re-elects itself — no manual trigger is
   needed.
3. An `ElectionMessage` (unique `electionId`) floods the neighbour graph; each worker processes a
   given election id **at most once** (duplicates are dropped), so cycles cannot loop forever.
   A per-worker `electionInProgress` guard stops a worker from starting its own election while it
   is participating in another one.
4. Each participant contributes a `CandidateInfo` (worker id + JAC) snapshot; echoed participant
   sets are merged back at the initiator, so **every reachable active worker** is considered.
5. Winner = **lowest JAC**; on a tie the **highest worker ID** wins.

**What data drives a fresh-election?** On a brand-new cluster every worker starts with
`JAC = 0`, so the first election has no JAC signal and the tie-break is used: all six workers tie
at `0`, so **worker 6** is elected. After each 5-job term the coordinator's own JAC has risen
(only it assigns sub-jobs to other workers), so the next election picks the lowest-JAC worker —
a **different** node each term. The coordinator therefore rotates `6 → 5 → 4 → 3 → 2 → 1 → 6 → …`,
and the rotation is always a *consequence* of the JAC values, never a fixed order.

6. The winner is broadcast to all participants and every worker records the same coordinator.

**Coordinator term:** a coordinator handles at most 5 submitted client jobs per term
(`jobsThisTerm`, separate from the lifetime JAC). Immediately after the 5th job completes it
demotes itself (`NO_COORDINATOR`) and starts a fresh election. A 6th submission during the
hand-over is refused until the new coordinator is announced; clients retry automatically against
the newly elected coordinator.

---

## Project Structure

```text
.
+-- pom.xml
+-- src/main/java/com/cs324
    +-- backend
    |   +-- api          (RMI contracts & DTOs: WorkerService, BootstrapService, messages)
    |   +-- bootstrap    (Bootstrap Node: BootstrapServer, BootstrapServiceImpl)
    |   +-- worker       (WorkerServer, WorkerServiceImpl, WorkerClusterConfig, launcher)
    |   +-- gui          (ServerGUI - manages the cluster)
    +-- frontend
        +-- client       (ClientGUI - submits jobs)
        +-- ui           (UITheme - shared dark look-and-feel)
```

## Requirements

- **JDK 17+** (compiled with `--release 17`; developed and tested on OpenJDK 17–26).
- **Maven 3.8+**.
- Any OS with Java + RMI; all examples assume a shell on `localhost`.

## Compile

```bash
mvn clean compile
```

Compiled classes land in `target/classes`. All launch commands below use:

```bash
java -cp target/classes <fully.qualified.ClassName>
```

---

## Run — Option A (Recommended): GUI End-to-End

Startup order is important — workers cannot register before the Bootstrap Node exists, and jobs
need an elected coordinator.

### 1. Start the Server Manager

```bash
java -cp target/classes com.cs324.backend.gui.ServerGUI
```

### 2. Start the Bootstrap Node

Click **Start Bootstrap**. The status line turns green: `Bootstrap: running ...` on port `1099`.

### 3. Start the six workers

Click **Start Workers**. The launcher spawns six JVMs (IDs `1..6`, RMI ports `5001..5006`,
logs `logs/worker-<id>.log`). The state table auto-refreshes and the dashboard should soon show
`● ONLINE` with `Workers online: 6/6`. Re-running is safe (already-running workers are detected
via pid files).

### 4. Wait for the automatic election

There is **no election button** — the workers elect a coordinator by themselves. Within ~10
seconds the dashboard shows `Election: COMPLETE` and **Elected Coordinator: Worker 6** (on a fresh
cluster all JACs are `0`, so the tie-break — highest worker ID — elects worker 6). The log line
`Election complete -> coordinator is worker 6` confirms it. After each 5-job term a new election
runs automatically and a different worker (lowest JAC) takes over.

### 5. Launch one or more Clients

In separate terminals (or more than once from anywhere) run:

```bash
java -cp target/classes com.cs324.frontend.client.ClientGUI
```

Click **Connect**. The client auto-discovers the coordinator through the Bootstrap Node and its
dashboard shows `● ONLINE` and `Coordinator: Worker N`. Pick a job type, type (or load) input,
press **Submit**. Multiple client GUIs can operate simultaneously.

### 6. Submit jobs

See [Job Types & Input Formats](#job-types--input-formats) below.

---

## Run — Option B: Manual Terminals

If you prefer raw processes instead of the Server GUI, each component can also be started from a
terminal. Everything is configurable; only the fixed worker port layout lives in
`WorkerClusterConfig`.

### 1. Bootstrap Node

```bash
java -cp target/classes com.cs324.backend.bootstrap.BootstrapServer
# default RMI registry port 1099; custom: ... BootstrapServer 2099
```

### 2. Workers (one JVM each)

```bash
java -cp target/classes com.cs324.backend.worker.WorkerServer 1 5001
java -cp target/classes com.cs324.backend.worker.WorkerServer 2 5002
java -cp target/classes com.cs324.backend.worker.WorkerServer 3 5003
java -cp target/classes com.cs324.backend.worker.WorkerServer 4 5004
java -cp target/classes com.cs324.backend.worker.WorkerServer 5 5005
java -cp target/classes com.cs324.backend.worker.WorkerServer 6 5006
```

Optional trailing args: `WorkerServer <workerId> <port> [bootstrapHost] [bootstrapPort] [workerHost]`.

### 3. Election

Nothing to do — each worker runs a background check and starts an election on its own once it
sees no coordinator. Start all six workers within a few seconds of each other and the cluster
self-elects.

### 4. Client

```bash
java -cp target/classes com.cs324.frontend.client.ClientGUI
```

---

## Job Types & Input Formats

| Job type | Input | Example | Result |
|----------|-------|---------|--------|
| `MAX` | comma-separated integers | `12,5,99,2,17` | largest number |
| `PRIMECOUNT` | comma-separated integers | `2,4,5,8,11` | number of primes (duplicates count) |
| `PRIMESUM` | `start,end` (inclusive range) | `1,1000` | sum of primes in the range |

### CSV formats

- `MAX` / `PRIMECOUNT`: one line of comma-separated integers, e.g. `12,5,99,2,17`.
- `PRIMESUM`: one line with two values, e.g. `1,1000`.

Malformed values (non-numeric, empty, `start > end`, `start < 1`) produce a clear error in the
task table instead of crashing.

The coordinator divides the input as evenly as possible across the reachable workers
(`base = n / w`, the first `n % w` workers get one extra item), ranking workers by **JAC** so the
least-loaded workers receive their slice first, e.g. a fresh `PRIMESUM(1, 1000)` on six workers
splits into contiguous ranges `w1:1-167  w2:168-334  …  w6:835-1000`. Partial results are merged
back at the coordinator, which returns the final answer to the requesting client.

### Ready-made sample CSV files

The project ships sample files in the `csv/` folder — open the client GUI, pick the matching job
type, press **Load CSV...**, choose a file and press **Submit**.

| File | Job type | Contents | Expected result |
|------|----------|----------|-----------------|
| `csv/max.csv` | `MAX` | `12,5,99,2,17,45,8,76,31,50,3,88` | `99` |
| `csv/max-small.csv` | `MAX` | `7,3,21,14,9,1,42` | `42` |
| `csv/primecount.csv` | `PRIMECOUNT` | `2,6,11,15,17,20,23,29,31,40,47,53,60,67,71,80` | `10` |
| `csv/primesum.csv` | `PRIMESUM` | `1,1000` | `76127` |
| `csv/primesum-range.csv` | `PRIMESUM` | `100,600` | `28236` |

## Stopping the Cluster Cleanly

- **Server GUI**: click **Stop Workers** (each worker unregisters via its shutdown hook), then
  close the GUI.
- **Manual**: `Ctrl+C` the worker terminals, then the Bootstrap terminal.
- Left-over pid/log files are cleaned automatically or re-detected on the next start.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `Connection refused` on worker/client start | Start the Bootstrap Node first; check the port numbers. |
| `Worker N is not the coordinator` | A term just ended and a new election is running; press Connect again or simply resubmit — the client auto-re-resolves the new coordinator. |
| `Coordinator term ended after 5 jobs ...` | Expected — the term expired; the cluster is re-electing and the next submission goes to the new coordinator. |
| Port already in use | Another instance is running (pid files skip it); stop it first. |
| GUI shows `ELECTING...` for a long time | Seconds-long is normal right after a term ends. If it persists, stop and restart the workers so they all re-run their staggered auto-election checks. |

## Known Limitations

- The neighbour graph is a ring backbone plus random connections, not a purely random graph; this
  deliberately guarantees reachability (so the cluster always agrees on one coordinator) while
  still satisfying the "random connection on join / unstructured subset" requirement.
- Worker discovery is centralised in the Bootstrap Node (documented decision); workers that are
  already running keep working if only the Bootstrap Node goes down, but new workers cannot join.
- The fixed cluster layout is 6 workers (IDs `1..6`, ports `5001..5006`).
- When starting workers manually one-by-one in separate terminals, start them within a few
  seconds of each other so the first automatic election covers the whole cluster.

## Tests

The repository previously shipped console test harnesses; these were replaced by the two GUIs:
`ServerGUI` exercises registration, election and status checks, and `ClientGUI` exercises all
three computation jobs (manual and CSV input, concurrent submissions, failure display, automatic
coordinator re-resolution after term hand-over). Run every scenario described above to validate
the system end-to-end.