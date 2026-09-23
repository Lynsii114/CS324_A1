# CS324_A1

## Bootstrap Node

This project currently implements the Bootstrap Node and the basic Worker Node
for the worker network using Java RMI.

The Bootstrap Node is a standalone Java RMI process that tracks active workers
and exposes these remote methods:

- `registerWorker(WorkerInfo worker)`
- `unregisterWorker(int workerId)`
- `getActiveWorkers()`
- `getRandomWorker()`

The Bootstrap Node only stores worker membership. It does not participate in
leader election and does not process jobs.

Each Worker Node runs as a separate Java RMI process with:

- a unique integer worker ID
- Job Allocation Counter (JAC) starting at `0`
- neighbour list
- current coordinator ID
- `leaderman = "cs324"`

When a Worker Node starts, it registers itself with the Bootstrap Node. Worker
leader election and job processing are not implemented yet.

### Project Structure

```text
.
+-- pom.xml
+-- backend
    +-- pom.xml
    +-- src
        +-- main
            +-- java
                +-- com
                    +-- cs324
                        +-- bootstrap
                        +-- worker
```

### Compile

From the repository root:

```powershell
mvn clean compile
```

### Run the Bootstrap Node

Start it in its own terminal:

```powershell
mvn -pl backend exec:java
```

The default RMI registry port is `1099`. To use another port:

```powershell
mvn -pl backend exec:java "-Dexec.args=2099"
```

### Test with the Included Client

With the Bootstrap Node still running, open another terminal and run:

```powershell
mvn -pl backend exec:java "-Dexec.mainClass=com.cs324.bootstrap.BootstrapClientTest"
```

For a custom host or port:

```powershell
mvn -pl backend exec:java "-Dexec.mainClass=com.cs324.bootstrap.BootstrapClientTest" "-Dexec.args='localhost 2099'"
```

The test client registers three workers, prints the active worker list, requests
a random worker, unregisters one worker, and prints the remaining active workers.

### Run a Worker Node

Start the Bootstrap Node first. Then open another terminal and run a worker:

```powershell
mvn -pl backend exec:java "-Dexec.mainClass=com.cs324.worker.WorkerServer" "-Dexec.args='1 5001'"
```

Arguments are:

```text
WorkerServer <workerId> <workerPort> [bootstrapHost] [bootstrapPort] [workerHost]
```

Example with an explicit Bootstrap Node address:

```powershell
mvn -pl backend exec:java "-Dexec.mainClass=com.cs324.worker.WorkerServer" "-Dexec.args='2 5002 localhost 1099 localhost'"
```

### Test a Worker Node

With the Bootstrap Node and a Worker Node running, inspect the worker over RMI:

```powershell
mvn -pl backend exec:java "-Dexec.mainClass=com.cs324.worker.WorkerClientTest" "-Dexec.args='localhost 5001 1'"
```

The worker test client prints the worker ID, JAC, coordinator ID, `leaderman`,
and verifies that neighbours can be added and removed.

### Test Leader Election with 6 Workers

This test does not implement or run computational jobs. All workers keep
`JAC=0`, so the election tie-break rule selects the highest worker ID:
worker `6`.

Open one terminal for the Bootstrap Node:

```powershell
mvn -pl backend exec:java
```

Open six more terminals, one per worker:

```powershell
mvn -pl backend exec:java "-Dexec.mainClass=com.cs324.worker.WorkerServer" "-Dexec.args='1 5001'"
mvn -pl backend exec:java "-Dexec.mainClass=com.cs324.worker.WorkerServer" "-Dexec.args='2 5002'"
mvn -pl backend exec:java "-Dexec.mainClass=com.cs324.worker.WorkerServer" "-Dexec.args='3 5003'"
mvn -pl backend exec:java "-Dexec.mainClass=com.cs324.worker.WorkerServer" "-Dexec.args='4 5004'"
mvn -pl backend exec:java "-Dexec.mainClass=com.cs324.worker.WorkerServer" "-Dexec.args='5 5005'"
mvn -pl backend exec:java "-Dexec.mainClass=com.cs324.worker.WorkerServer" "-Dexec.args='6 5006'"
```

After all six workers have registered, open one more terminal and run:

```powershell
mvn -pl backend exec:java "-Dexec.mainClass=com.cs324.worker.WorkerElectionTest"
```

The test client connects workers in a ring:

```text
1 -- 2 -- 3 -- 4 -- 5 -- 6
|                         |
+-------------------------+
```

Expected flow:

```text
Bootstrap
    ↓
6 Workers register
    ↓
Workers connect as neighbours
    ↓
Election starts at worker 1
    ↓
ELECTION messages compare worker IDs and JAC values
    ↓
All JAC values are 0, so the tie chooses highest ID
    ↓
Worker 6 is selected as coordinator
    ↓
COORDINATOR messages propagate
    ↓
All reachable workers report coordinatorId=6
```

Look for these console messages:

- Bootstrap terminal: `[Bootstrap] Registered worker: ...`
- Worker terminals: `NEIGHBOUR connected`, `ELECTION received`,
  `ELECTION forwarding`, `COORDINATOR broadcast`,
  `COORDINATOR received`, and `COORDINATOR forwarding`
- Test terminal: final coordinator view showing every worker with
  `coordinatorId=6`

### Test Distributed MAX Job

Run the 6-worker leader election test first so all reachable workers agree on
the coordinator. With the default test values, worker `6` becomes coordinator.

Then send a MAX job to worker `6`:

```powershell
mvn -pl backend exec:java "-Dexec.mainClass=com.cs324.worker.WorkerMaxClientTest"
```

To provide your own comma-separated numbers:

```powershell
mvn -pl backend exec:java "-Dexec.mainClass=com.cs324.worker.WorkerMaxClientTest" "-Dexec.args='localhost 5006 6 4,17,2,99,31,8'"
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
