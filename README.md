# CS324_A1

## Overview

This project implements a Java RMI distributed computing cluster for CS324.
It contains:

- a neutral Bootstrap Node that tracks active workers
- four Worker Nodes, IDs `1` through `4`
- a random connected neighbour topology for worker-to-worker messages
- a distributed leader election protocol
- threaded job dispatch from the elected leader to reachable workers
- a Swing GUI for viewing workers, starting workers, starting elections, and submitting jobs

The Bootstrap Node is only a registry. It does not participate in elections,
does not execute jobs, and does not assign work.

## Project Structure

```text
.
+-- pom.xml
+-- README.md
+-- src/main/java/com/cs324
    +-- backend
    |   +-- api          RMI contracts and shared DTOs
    |   +-- bootstrap    Bootstrap node implementation
    |   +-- worker       Worker node, cluster config, and launcher
    +-- frontend
        +-- client       Swing GUI only
```

Generated folders such as `target/`, `logs/`, `out/`, and `.class` files are
ignored by Git and should not be committed.

## Cluster Layout

The cluster is configured in `WorkerClusterConfig`.

| Worker ID | RMI registry port | RMI binding name           | Log file             | PID file            |
|-----------|-------------------|----------------------------|----------------------|---------------------|
| 1         | 5001              | `WorkerService-1`          | `logs/worker-1.log`  | `logs/worker-1.pid` |
| 2         | 5002              | `WorkerService-2`          | `logs/worker-2.log`  | `logs/worker-2.pid` |
| 3         | 5003              | `WorkerService-3`          | `logs/worker-3.log`  | `logs/worker-3.pid` |
| 4         | 5004              | `WorkerService-4`          | `logs/worker-4.log`  | `logs/worker-4.pid` |

With all four workers online, each worker has two random-ring neighbours.

## Compile

Use Maven when it is available:

```powershell
mvn clean compile
```

If Maven is not installed, compile directly with `javac`:

```powershell
javac -d target/classes (Get-ChildItem src\main\java -Recurse -Filter *.java | ForEach-Object { $_.FullName })
```

## Run

Start the Bootstrap Node:

```powershell
java -cp target/classes com.cs324.backend.bootstrap.BootstrapServer
```

Start all four workers:

```powershell
java -cp target/classes com.cs324.backend.worker.WorkerClusterLauncher start
```

Open the Swing GUI:

```powershell
java -cp target/classes com.cs324.frontend.client.DistriLabClientGui
```

The GUI shows:

- system status
- the current leader
- jobs submitted in the current term
- one card per worker
- a `Start` button on each worker card
- global `Start Election` and `Submit Job` buttons

Use the GUI's `Start Election` button to elect the leader. Because all workers
start with `JAC=0`, the highest worker ID wins the default tie-break. In the
four-worker cluster, worker `4` becomes leader.

## Expected Demonstration Flow

1. Start the Bootstrap Node.
2. Start Worker 1, Worker 2, Worker 3, and Worker 4 as separate JVM processes.
3. Watch the Bootstrap console print each worker registration.
4. Watch each worker print `RANDOM neighbours -> ...` as it forms random-ring neighbours.
5. Start an election and watch `ELECTION forwarding` / `ELECTION received` messages spread.
6. With all JAC values at `0`, Worker 4 wins because the highest ID breaks the tie.
7. If Worker 4's JAC is raised, the next lowest-JAC tie is Workers 1-3, so Worker 3 wins.
8. Watch `COORDINATOR received` / `COORDINATOR forwarding` messages spread until every worker stores the same leader.

## Stop

Stop worker processes:

```powershell
java -cp target/classes com.cs324.backend.worker.WorkerClusterLauncher stop
```

Stop the Bootstrap Node with `Ctrl+C` in its terminal.

## Election Rules

- Workers refresh their neighbour set from the Bootstrap registry.
- Workers form a connected random-ring topology and log their selected neighbours.
- Election messages are deduplicated by UUID.
- The winner is the reachable worker with the lowest JAC.
- Ties are broken by the highest worker ID.
- The winner is announced to all reachable workers.

## Threaded Job Execution

When the leader receives a MAX, PRIMESUM, or PRIMECOUNT job, it splits the input
across reachable workers and uses a Java `ExecutorService` to submit each chunk
on a separate dispatch thread. The leader waits on the returned `Future`
objects, then combines the partial results.

## Coordinator Terms

The coordinator tracks completed jobs in the current term. Each coordinated job
also increases the coordinator's own JAC, so the same leader is less likely to
win repeatedly. After the fifth completed job, it logs `TERM ended`, resets
coordinator state across reachable workers, and automatically starts a new
election. The next coordinator is again chosen by lowest JAC, with highest
worker ID breaking ties.
