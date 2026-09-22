# CS324_A1

## Bootstrap Node

This project currently implements the Bootstrap Node and the basic Worker Node
for the worker network using Java RMI, plus a launcher that runs a fixed
six-worker cluster (worker IDs `1` through `6`).

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
java -cp target/classes com.cs324.frontend.client.ClusterStatusClient
```

Expected output ends with:

```text
6/6 workers reachable over RMI
```

You can also inspect a single worker directly, e.g. worker 6:

```powershell
java -cp target/classes com.cs324.frontend.client.WorkerClientTest localhost 5006 6
```

#### Step 5 - Monitor or stop the workers

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
