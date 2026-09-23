# CS324_A1

## Bootstrap Node

This project implements the Bootstrap Node, the Worker Nodes and a distributed
leader election protocol for a six-worker cluster (worker IDs `1` through `6`),
all communicating through Java RMI.

The Bootstrap Node is a standalone Java RMI process that tracks active workers
and exposes these remote methods:

- `registerWorker(WorkerInfo worker)`
- `unregisterWorker(int workerId)`
- `getActiveWorkers()`
- `getRandomWorker()`

The Bootstrap Node is strictly neutral. It serves only as a central registry of
active workers. It never participates in leader elections, never executes jobs
and never assigns tasks; job execution and leadership are reserved for the
worker nodes.

Each Worker Node runs as a separate Java RMI process with:

- a unique integer worker ID
- Job Allocation Counter (JAC) starting at `0`
- a neighbour ring (each worker links to its predecessor and successor in the
  registry, so the ring grows dynamically as workers register)
- current coordinator ID, initially `-1` (none)
- `leaderman = "cs324"`
- a thread-safe set of processed election IDs for deduplication

When a Worker Node starts, it registers itself with the Bootstrap Node and
rebuilds its neighbour ring from the active-worker registry.

### Leader Election

Any worker can initiate a leader election while no coordinator is present
(coordinator ID `-1`). The algorithm fulfils the following requirements:

- **Message propagation** - An `ELECTION` message is forwarded hop-by-hop
  through neighbouring workers over the ring. Each worker re-sends the message
  to its neighbours (except the hop it arrived from) and echoes back the merged
  set of discovered participants, so messages terminate cleanly.
- **Deduplication** - Every election gets a unique UUID election id. Every
  worker maintains a thread-safe set (`ConcurrentHashMap.newKeySet()`) of
  processed election ids; a message whose election id was already handled is
  dropped, so no message is ever processed twice and message loops are cut.
- **Network scope** - The election discovers every currently reachable, active
  worker before a winner is chosen. Because each participating worker refreshes
  its ring from the Bootstrap registry, newly joined workers are discovered as
  well. The winner is selected only after all echoes have returned.
- **Role separation** - The Bootstrap Node only hands out the active-worker
  registry. Elections, winner selection and coordination happen entirely
  between worker nodes over RMI.
- **Tie-breaking** - Among the reachable workers, the one with the lowest JAC
  wins (JAC counts allocated jobs, so lower means more free capacity). If two
  or more workers tie on the same lowest JAC, the worker with the highest
  unique Worker ID is elected. With the default cluster (every JAC = `0`)
  worker `6` therefore wins.

After the winner is chosen, the initiator broadcasts a `WinnerAnnouncement` to
every participant, so all workers agree on the same coordinator. Initiating an
election while a coordinator is already present is refused; reset the
coordinators first if you want to re-run (see the `reset` command below).

### Six-Worker Cluster (IDs 1-6)

The cluster layout is fixed in `WorkerClusterConfig`:

| Worker ID | RMI registry port | RMI binding name           | Log file             | Pid file            |
|-----------|-------------------|----------------------------|----------------------|---------------------|
| 1         | 5001              | `WorkerService-1`          | `logs/worker-1.log`  | `logs/worker-1.pid` |
| 2         | 5002              | `WorkerService-2`          | `logs/worker-2.log`  | `logs/worker-2.pid` |
| 3         | 5003              | `WorkerService-3`          | `logs/worker-3.log`  | `logs/worker-3.pid` |
| 4         | 5004              | `WorkerService-4`          | `logs/worker-4.log`  | `logs/worker-4.pid` |
| 5         | 5005              | `WorkerService-5`          | `logs/worker-5.log`  | `logs/worker-5.pid` |
| 6         | 5006              | `WorkerService-6`          | `logs/worker-6.log`  | `logs/worker-6.pid` |

Every worker is an independent JVM process with its own RMI registry port, its
own log file and its own pid file, so the six workers are isolated from each
other. They still communicate through Java RMI: each worker binds its
`WorkerService` in its own registry and registers with the Bootstrap Node, and
any worker (or client) can call any other worker through
`rmi://<host>:<port>/WorkerService-<id>`.

### Project Structure

```text
.
+-- pom.xml
+-- src/main/java/com/cs324
    +-- backend
    |   +-- api          (RMI contracts & shared DTOs)
    |   +-- bootstrap    (Bootstrap Node)
    |   +-- worker       (Worker Node, cluster config & launcher)
    +-- frontend
        +-- client       (manual test harnesses)
```

### Compile

From the repository root:

```powershell
mvn clean compile
```

Compiled classes land in `target/classes`. All run commands below launch the
corresponding `main` with the JVM directly:

```powershell
java -cp target/classes <fully.qualified.ClassName> [args...]
```

### Run the Bootstrap Node

Start it in its own terminal:

```powershell
java -cp target/classes com.cs324.backend.bootstrap.BootstrapServer
```

The default RMI registry port is `1099`. To use another port:

```powershell
java -cp target/classes com.cs324.backend.bootstrap.BootstrapServer 2099
```

The Bootstrap Node keeps running and waits for workers to register.

### Test with the Included Client

With the Bootstrap Node still running, open another terminal and run:

```powershell
java -cp target/classes com.cs324.frontend.client.BootstrapClientTest
```

For a custom host or port:

```powershell
java -cp target/classes com.cs324.frontend.client.BootstrapClientTest localhost 2099
```

The test client registers three workers, prints the active worker list, requests
a random worker, unregisters one worker, and prints the remaining active workers.

### Run a Worker Node

Start the Bootstrap Node first. Then open another terminal and run a worker:

```powershell
java -cp target/classes com.cs324.backend.worker.WorkerServer 1 5001
```

Arguments are:

```text
WorkerServer <workerId> <workerPort> [bootstrapHost] [bootstrapPort] [workerHost]
```

Example with an explicit Bootstrap Node address:

```powershell
java -cp target/classes com.cs324.backend.worker.WorkerServer 2 5002 localhost 1099 localhost
```

### Test a Worker Node

With the Bootstrap Node and a Worker Node running, inspect the worker over RMI:

```powershell
java -cp target/classes com.cs324.frontend.client.WorkerClientTest localhost 5001 1
```

The worker test client prints the worker ID, JAC, coordinator ID, `leaderman`,
and verifies that neighbours can be added and removed.

### Step-by-Step: Initialize and Start All Six Worker Processes

Prerequisites: Java 17+ and Maven installed, and the project compiled.

#### Step 1 - Compile the project

From the repository root:

```powershell
mvn clean compile
```

#### Step 2 - Start the Bootstrap Node

Workers cannot register without it, so start it first in its own terminal:

```powershell
java -cp target/classes com.cs324.backend.bootstrap.BootstrapServer
```

Leave this terminal running (default RMI registry port `1099`).

#### Step 3 - Start the six worker processes

Open a second terminal in the repository root and run the launcher:

```powershell
java -cp target/classes com.cs324.backend.worker.WorkerClusterLauncher
```

The launcher spawns six separate JVM processes, one per worker ID `1`-`6`,
each with:

- its own RMI registry on ports `5001`-`5006`
- its own log file `logs/worker-<id>.log` (the launcher creates `logs/` if missing)
- its own pid file `logs/worker-<id>.pid`
- registration with the Bootstrap Node over Java RMI

After starting, the launcher performs an RMI lookup against every worker and
prints one `Verified over RMI: worker <id> ...` line per worker, ending with
`Cluster state: 6/6 workers running`.

Workers that are already running are detected via their pid file and skipped,
so re-running the command is safe. To point the workers at a Bootstrap Node on
a custom port, pass it as an argument:

```powershell
java -cp target/classes com.cs324.backend.worker.WorkerClusterLauncher start localhost 2099
```

#### Step 4 - Verify the cluster

In a third terminal:

```powershell
java -cp target/classes com.cs324.frontend.client.ClusterStatusClient status
```

Expected output ends with:

```text
6/6 workers reachable over RMI
No coordinator elected yet - run: ClusterStatusClient election [initiatorId]
```

You can also inspect a single worker directly, e.g. worker 6:

```powershell
java -cp target/classes com.cs324.frontend.client.WorkerClientTest localhost 5006 6
```

#### Step 5 - Run a leader election

With no coordinator present, initiate an election from any worker (here worker
1):

```powershell
java -cp target/classes com.cs324.frontend.client.ClusterStatusClient election 1
```

Because every worker starts with JAC `0`, the six workers tie and the highest
worker ID wins:

```text
Election result: Coordinator elected: worker 6 (JAC=0) across 6 reachable workers [electionId=...]
...
Cluster is coordinated by worker 6
```

All six workers now report `coordinator=6`. While a coordinator is present, a
new election is refused. Use `reset` (followed by `election`) to re-run:

- Bootstrap terminal: `[Bootstrap] Registered worker: ...`
- Worker terminals: `NEIGHBOUR connected`, `ELECTION received`,
  `ELECTION forwarding`, `COORDINATOR broadcast`,
  `COORDINATOR received`, and `COORDINATOR forwarding`
- Test terminal: final coordinator view showing every worker with
  `coordinatorId=6`

```powershell
java -cp target/classes com.cs324.frontend.client.ClusterStatusClient reset
```

### Test Distributed MAX Job

Run the 6-worker leader election test first so all reachable workers agree on
the coordinator. With the default test values, worker `6` becomes coordinator.

Then send a MAX job to worker `6`:

```powershell
java -cp target/classes com.cs324.frontend.client.WorkerMaxClientTest
```

To provide your own comma-separated numbers:

```powershell
java -cp target/classes com.cs324.frontend.client.WorkerMaxClientTest localhost 5006 6 "4,17,2,99,31,8"
```

Expected flow:

```text
Client
    ↓
Coordinator receives MAX(numbers)
    ↓
Coordinator finds reachable workers
    ↓
Coordinator divides the list as evenly as possible
    ↓
Each worker computes a partial maximum for its section
    ↓
Coordinator combines partial maximums
    ↓
Client receives final maximum
```

Look for these console messages:

- Client terminal: `[Client] Sending MAX job` and `[Client] MAX result`
- Coordinator terminal: `MAX job received`, `MAX assigning`,
  `MAX partial result`, and `MAX final result`
- Worker terminals: `MAX partial compute`

#### Step 6 - Monitor or stop the workers

Check which worker processes are alive:

```powershell
java -cp target/classes com.cs324.backend.worker.WorkerClusterLauncher status
```

Tail a single worker's log (each worker is fully isolated in its own file):

```powershell
Get-Content logs/worker-1.log -Wait
```

Stop all six worker processes cleanly (each also unregisters from the
Bootstrap Node through its shutdown hook):

```powershell
java -cp target/classes com.cs324.backend.worker.WorkerClusterLauncher stop
```

Stop the Bootstrap Node by interrupting its terminal (`Ctrl+C`).

#### Starting the workers manually (alternative)

Instead of the launcher you can start each worker in its own terminal. Run the
Bootstrap Node first, then open six terminals and run one command in each:

```powershell
java -cp target/classes com.cs324.backend.worker.WorkerServer 1 5001
java -cp target/classes com.cs324.backend.worker.WorkerServer 2 5002
java -cp target/classes com.cs324.backend.worker.WorkerServer 3 5003
java -cp target/classes com.cs324.backend.worker.WorkerServer 4 5004
java -cp target/classes com.cs324.backend.worker.WorkerServer 5 5005
java -cp target/classes com.cs324.backend.worker.WorkerServer 6 5006
```

With an explicit Bootstrap Node address:

```powershell
java -cp target/classes com.cs324.backend.worker.WorkerServer 6 5006 localhost 1099 localhost
```

### ClusterStatusClient Reference

`ClusterStatusClient` drives demos and verification of the six-worker cluster:

| Command | Purpose |
|---------|---------|
| `status [host] [bootstrapPort]` | Print Bootstrap registration and per-worker state (JAC, coordinator, neighbour count). |
| `election [initiatorId] [host] [bootstrapPort]` | Have the given worker (default `1`) start a leader election, then re-print status. |
| `reset [host] [bootstrapPort]` | Set every worker's coordinator back to `-1`, so a new election can be run. |
| `bump-jac <workerId> <amount> [host] [bootstrapPort]` | Increase a worker's JAC to exercise JAC-priority tie-breaking. |

### Tie-Breaking Example

The election prefers the lowest JAC. To prove it, raise worker 6's JAC so its
JAC is no longer the minimum, then re-elect:

```powershell
java -cp target/classes com.cs324.frontend.client.ClusterStatusClient reset
java -cp target/classes com.cs324.frontend.client.ClusterStatusClient bump-jac 6 3
java -cp target/classes com.cs324.frontend.client.ClusterStatusClient election 5
```

This time the winner is worker `5` (`JAC=0`): workers `1`-`5` are tied on the
lowest JAC and the tie is broken by the highest worker ID:

```text
Election result: Coordinator elected: worker 5 (JAC=0) across 6 reachable workers [electionId=...]
```

### Coordinator Job-Allocation Tracking (JAC)

While a worker is coordinator, every section it delegates to another worker is a
job allocation: the coordinator's Job Acceptance Counter (JAC) is incremented
for each remote assignment via `recordJobAllocation()`. The coordinator's own
section is not counted as an allocation.

During a MAX job, each delegated remote section increments the coordinator's
JAC. Look for these messages on the coordinator terminal:

```text
[Worker 6] JAC changed to 1 (assigned a job section to another worker)
[Worker 6] JAC changed to 2 (assigned a job section to another worker)
```

Because elections prefer the worker with the lowest JAC, JAC growth directly
influences future leadership: after a coordinator has delegated enough work,
`reset` the coordinators and run a new election to let lower-JAC workers
become coordinator (see the tie-breaking example above).

### Watching the Election Propagate

Each worker logs its election activity in its own file. During an election you
can watch the ELECTION messages hop around the ring and see deduplication in
action:

```text
[Worker 1] election <electionId> started, JAC=0, neighbours=2
[Worker 2] dropping duplicate election message <electionId> from worker 3
...
[Worker 6] dropping duplicate election message <electionId> from worker 1
[Worker 1] election <electionId> winner: worker 6 (JAC=0) across 6 reachable workers
```

Because each worker processes an election id at most once, the second arrival of
every message is dropped and the flood terminates after every reachable worker
has been discovered once.

### Why the Bootstrap Node Stays Neutral

The Bootstrap Node's log only ever shows registrations and unregistrations:

```text
[Bootstrap] Registered worker: WorkerInfo{workerId=1, host='localhost', port=5001}
...
```

Elections, winner selection and coordination are entirely between worker nodes:
the bootstrap merely answers `getActiveWorkers()`, which the workers use to
rebuild their neighbour ring and discover each other over RMI.
