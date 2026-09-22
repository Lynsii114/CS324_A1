# CS324_A1

## Bootstrap Node

This project currently implements only the Bootstrap Node for the worker network.
It is a standalone Java RMI process that tracks active workers and exposes these
remote methods:

- `registerWorker(WorkerInfo worker)`
- `unregisterWorker(String workerId)`
- `getActiveWorkers()`
- `getRandomWorker()`

The Bootstrap Node only stores worker membership. It does not participate in
leader election and does not process jobs.

### Project Structure

```text
.
├── pom.xml
└── backend
    ├── pom.xml
    └── src
        └── main
            └── java
                └── com
                    └── cs324
                        └── bootstrap
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
