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
