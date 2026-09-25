# Distributed Computing Cluster

## Project Overview

This project is a distributed computing cluster built with **Java RMI**. These are **six independent worker processes** connected in an unstructured network, a neutral
**Bootstrap Node** tracking membership, and a distributed **leader election** that lets the
workers agree on a **Coordinator** for one term at a time. Clients submit jobs (MAX, PRIMECOUNT,
PRIMESUM); the coordinator re-splits the work into a **variable number of contiguous segments**
(based on the task size) and RANKS the workers by **JAC** (lowest first) so the least-loaded nodes
get work first — each worker that processes a segment has its own JAC incremented — merges the
partial results and returns the final answer.

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

- **Bootstrap Node** — only tracks active workers and sequences startup-lottery claims; it never
  runs jobs and never participates in elections. Exposes `registerWorker`, `unregisterWorker`,
  `getActiveWorkers`, `getRandomWorker`, `lotteryClaim`, `getPriorityTable`.
- **Worker** — registers with the Bootstrap Node, maintains neighbours, participates in the
  startup lottery and in elections, and runs distributed computations. Tracks a lifetime **JAC**
  (segments processed) and a per-term submitted-job counter.
- **Coordinator** — a worker elected for one term. Receives jobs from clients, finds reachable
  active workers, splits each task into segments of a fixed maximum size, hands them to the
  lowest-JAC workers (incrementing the receiving worker's JAC), combines partial results, assigns
  at most **five jobs per term**, then broadcasts Term_End and steps down.
- **Client** — a separate process with a GUI that accepts manual or CSV input and submits jobs
  concurrently.

---

## Election, Startup Lottery & Tie-Breaking Rules

The election protocol runs in three phases.

### Phase 1 — Startup & Initial Leader Election (Timer Lottery)

1. All six workers boot with `JAC = 0` and no coordinator.
2. When the Bootstrap Node reports the cluster complete (6/6 registered), every worker starts a
   **randomized countdown of 150–300 ms**.
3. The first worker whose countdown expires claims **startup priority 6**, the second claims
   **priority 5**, … the last claims **priority 1**. The Bootstrap Node sequences the claims, so
   every cold start produces priority ids `1..6` in a random assignment.
4. The worker holding **priority 6** declares itself the **Initial Coordinator** and alerts the
   other five; all empty JACs mean there is no better signal, so the lottery winner leads.

### Phase 2 — Dynamic Segmented Task Distribution

5. Reachable workers are ranked by **JAC (ascending)** — least-loaded first — then by
   startup priority (descending), then worker id.
6. Each submitted task is split into a **variable number of contiguous segments**:
   `max(1, min(activeWorkers, ⌈items / maxItemsPerSegment⌉))`. A large PRIMESUM spreads over many
   workers, a small one over one or two — segment counts differ from task to task.
7. Each segment is handed to the lowest-JAC worker, and **that worker's own JAC is incremented**
   (including for a segment the coordinator keeps itself). The coordinator can therefore process
   several clients concurrently, and the JAC values diverge naturally with workload.

### Phase 3 — Term Expiration & Lowest-JAC Re-Election

8. A coordinator serves **at most 5 submitted client jobs** per term (`jobsThisTerm`). Immediately
   after the 5th job it pauses, broadcasts **Term_End** containing its verified final JAC table,
   demotes itself to `NO_COORDINATOR`, and starts a fresh election.
9. The new coordinator is the reachable worker with the **lowest JAC**; on a tie the **highest
   startup priority id** wins (worker id as a final safety net). The new coordinator's term counter
   resets to 0 and it serves its own 5 jobs (its *own* JAC is never reset — it keeps climbing with
   the segments it processes). When a term ends the outgoing coordinator takes a small **demotion
   tick** (`JAC + 1`) after broadcasting Term_End, so a cluster in which every worker processed the
   same number of segments (e.g. a PRIMECOUNT that fans out to all six nodes every time) still
   rotates to a least-loaded worker instead of re-electing the same node forever.
10. A 6th submission during the hand-over is refused until the new coordinator is announced;
    clients retry automatically against the newly elected coordinator.

**Election mechanics (used from Phase 3 onward, and as a fallback whenever no coordinator is
present):** any worker can initiate an election; the fixed background check runs quietly and
starts one when the cluster has no coordinator. An `ElectionMessage` (unique `electionId`) floods
the neighbour graph; each worker processes a given election id **at most once** (duplicates are
dropped), and a per-worker `electionInProgress` guard stops a worker from starting its own election
while it is participating in another one. Each participant contributes a `CandidateInfo` (worker id
+ priority id + JAC) snapshot; echoed participant sets are merged back at the initiator, so every
reachable active worker is considered. The winner is broadcast to all participants and every worker
records the same coordinator.

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

There is **no election button** — the six workers run a startup timer lottery and elect a
coordinator by themselves. Within ~10 seconds the dashboard shows `Election: COMPLETE` and
**Elected Coordinator: Worker N**. The worker whose 150–300 ms countdown expired first claimed
priority 6 and declared itself the Initial Coordinator (all JACs are 0 at boot). The Startup
Lottery column shows the minted priority per worker. After each 5-job term the coordinator
broadcasts Term_End with its final JAC table and a lowest-JAC election runs automatically.

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

The coordinator splits each task into a **variable number of contiguous segments** for the
reachable workers — MAX/PRIMECOUNT use `⌈n / 3⌉` segments, PRIMESUM uses `⌈range / 200⌉` —
ranking workers by **JAC** (least-loaded first) and incrementing the receiving worker's JAC for
each segment. A fresh `PRIMESUM(1, 1000)` therefore splits into five ranges of ~200 numbers and a
10-number MAX into four groups. Partial results are merged back at the coordinator, which returns
the final answer to the requesting client.

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
