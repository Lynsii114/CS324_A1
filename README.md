# CS324 A1 — Distributed Computing Cluster (Bootstrap / Workers / Election)

## Project Overview

This project is a distributed computing cluster built with **Java RMI**. Four independent
worker processes perform computation, a neutral **Bootstrap Node** tracks membership, and a
distributed **leader election** lets the workers agree on a **Coordinator** for one term at a
time. Clients submit jobs (MAX, PRIMECOUNT, PRIMESUM); the coordinator splits the work evenly
across all reachable active workers, merges the partial results and returns the final answer.

Everything ships with two Swing GUIs:

- **ServerGUI** — starts the Bootstrap Node and the four worker processes and shows a live
  dashboard (network ONLINE/PARTIAL/OFFLINE, elected coordinator, election status). Workers elect
  a coordinator **automatically in the background** — no manual trigger needed.
- **ClientGUI** — submits jobs with manual or CSV input, supports concurrent submissions, and
  tracks each job in a task table. Multiple client GUIs can run at the same time.

Each worker runs in its own JVM with its own RMI registry port, its own log file and its own
pid file. Workers only know their direct neighbours (a ring built from the Bootstrap registry),
but elections and job distribution reason over **all reachable active workers** by forwarding
messages hop-by-hop over the neighbour ring.

---

## Assignment Requirements

### Functional Requirements

| # | Requirement | Status | Where implemented |
|---|-------------|--------|-------------------|
| 1 | Start the Bootstrap Node independently | ✔ | `BootstrapServer`, `ServerGUI` |
| 2 | Register and unregister workers | ✔ | `BootstrapServiceImpl.registerWorker / unregisterWorker` |
| 3 | Assign every worker a unique integer ID | ✔ | config-driven IDs `1..4` (`WorkerClusterConfig`) |
| 4 | Randomly connect a joining worker to an existing active worker | ✔ | `BootstrapServiceImpl.getRandomWorker()`; workers rebuild their ring from the registry |
| 5 | Maintain an unstructured neighbour ring | ✔ | `WorkerServiceImpl.neighbours` + `refreshNeighbours()` |
| 6 | Detect when no valid coordinator exists | ✔ | `NO_COORDINATOR = -1`, `initiateElection()` guard |
| 7 | Initiate and propagate elections | ✔ | `initiateElection()` / `receiveElection()` hop-by-hop flooding |
| 8 | Prevent duplicate election processing | ✔ | thread-safe `processedElectionIds` set, inserted before processing |
| 9 | Compare candidates: lowest JAC wins, tie → highest ID | ✔ | `selectWinner(...)` |
| 10 | Propagate the coordinator result | ✔ | `WinnerAnnouncement` broadcast to all participants |
| 11 | End a coordinator term after five assigned jobs | ✔ | `jobsThisTerm` limit + `maybeEndCoordinatorTerm()` triggers re-election |
| 12 | Support MAX, PRIMESUM, PRIMECOUNT | ✔ | `submitMaxJob`, `submitPrimeSum`, `submitPrimeCount` |
| 13 | Split jobs evenly across active workers | ✔ | balanced chunking (`base = n/w`, remainder distributed) |
| 14 | Process multiple jobs concurrently | ✔ | bounded per-worker `ExecutorService` |
| 15 | Manual and CSV input in the client GUI | ✔ | `ClientGUI` input area + "Load CSV…" |
| 16 | Support simultaneous clients | ✔ | stateless RMI client; coordinator merges jobs via its executor |

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
   elects by itself, and a cluster whose coordinators were reset re-elects within a few seconds —
   no manual trigger is needed.
3. An `ElectionMessage` (unique `electionId`) floods the neighbour ring; each worker processes a
   given election id **at most once** (duplicates are dropped), so cycles cannot loop forever.
   A per-worker `electionInProgress` guard stops a worker from starting its own election while it
   is participating in another one.
4. Each participant contributes a `CandidateInfo` (worker id + JAC) snapshot; echoed participant
   sets are merged back at the initiator, so **every reachable active worker** is considered.
5. Winner = **lowest JAC**; on a tie the **highest worker ID** wins. With a fresh cluster all
   JACs are `0`, so worker `4` is elected.
6. The winner is broadcast to all participants and every worker records the same coordinator.

**Coordinator term:** a coordinator handles at most 5 submitted client jobs per term
(`jobsThisTerm`, separate from the lifetime JAC). Immediately after the 5th job completes it
demotes itself (`NO_COORDINATOR`) and starts a fresh election. A 6th submission during the
hand-over is refused until the new coordinator is announced.

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

### 3. Start the four workers

Click **Start Workers**. The launcher spawns four JVMs (IDs `1..4`, RMI ports `5001..5004`,
logs `logs/worker-<id>.log`). The state table auto-refreshes and the dashboard should soon show
`● ONLINE` with `Workers online: 4/4`. Re-running is safe (already-running workers are detected
via pid files).

### 4. Wait for the automatic election

There is **no election button** — the workers elect a coordinator by themselves. Within ~10
seconds the dashboard shows `Election: COMPLETE` and **Elected Coordinator: Worker 4** (all JACs
are `0`, so the highest worker ID wins). The log line `Election complete -> coordinator is worker 4`
confirms it. If you click **Reset Coordinators**, the workers re-elect themselves a few seconds
later.

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
```

Optional trailing args: `WorkerServer <workerId> <port> [bootstrapHost] [bootstrapPort] [workerHost]`.

### 3. Election

Nothing to do — each worker runs a background check and starts an election on its own once it
sees no coordinator. Start all four workers within a few seconds of each other and the cluster
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
(`base = n / w`, the first `n % w` workers get one extra item) and combines the partial results.

---

## Observing Internals

- **Worker logs**: `logs/worker-<id>.log` hold the full inter-worker message trail
  (`election ... started`, `ELECTION forwarding`, `dropping duplicate election message ...`,
  `auto-election result`, coordinator announcements, and per-slot JAC updates).
- **Election demonstration**: start the workers and watch the Server GUI dashboard flip to
  `ELECTING...` then `COMPLETE` with the elected node; the WinnerAnnouncement is identical on all
  four workers for a given election id.
- **Five-job term change**: submit five jobs through the client; the coordinator log then shows
  `completed 5 jobs this term - ending term and starting a new leader election` and a new
  coordinator is elected (usually a lower-JAC worker).
- **Concurrency**: submit several jobs at once from two client GUIs and watch the per-worker
  executor thread names (`job-<id>-<n>`) in the logs.

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
| `Cannot accept submission: not the coordinator` | A new term/election is running; reconnect the client after the dashboard shows a new coordinator. |
| `Coordinator term ended after 5 jobs ...` | Expected — the term expired; the client got the response and the cluster is re-electing. |
| Port already in use | Another instance is running (pid files skip it); stop it first. |
| GUI shows `ELECTING...` for a long time | The workers cannot agree; press **Reset Coordinators** and the workers re-elect automatically. |

## Known Limitations

- The neighbour graph is a ring, not a random unstructured graph; election flooding and
  reachability probing still exercise arbitrary forwarding over the ring.
- Worker discovery is centralised in the Bootstrap Node (documented decision); workers that are
  already running keep working if only the Bootstrap Node goes down, but new workers cannot join.
- The fixed cluster layout is 4 workers (IDs `1..4`, ports `5001..5004`) per the assignment.
- When starting workers manually one-by-one in separate terminals, start them within a few
  seconds of each other so the first automatic election covers the whole cluster.

## Tests

The repository previously shipped console test harnesses; these were replaced by the two GUIs:
`ServerGUI` exercises registration, election, reset and status checks, and `ClientGUI` exercises
all three computation jobs (manual and CSV input, concurrent submissions, failure display).
Run every scenario described above to validate the system end-to-end.