package com.cs324.frontend.launcher;

import com.cs324.backend.api.WorkerService;
import com.cs324.backend.api.WorkerState;
import com.cs324.backend.bootstrap.BootstrapServer;
import com.cs324.backend.worker.WorkerServer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Orchestrator for the six-worker network.
 *
 * <p>This process does not act as a worker itself. It reads the worker topology
 * from <code>worker-network.properties</code>, starts one independent JVM per
 * Worker Node (IDs 1-6), and then wires a neighbour ring between them over
 * Java RMI. Every worker runs in its own isolated process and owns its own RMI
 * registry, so stopping the launcher (Ctrl+C) or running {@code stop} shuts
 * down all spawned worker processes.
 */
public class WorkerLauncher {

    public static final String CONFIG_RESOURCE = "worker-network.properties";
    public static final String DEFAULT_LOG_DIR = "logs";
    private static final long WIRE_DELAY_MS = 2000;

    private static final class WorkerConfig {
        int id;
        String host;
        int port;
    }

    public static void main(String[] args) throws Exception {
        int argIndex = 0;
        String command = "start";
        if (args.length > 0 && (args[0].equals("start") || args[0].equals("stop"))) {
            command = args[0];
            argIndex = 1;
        }

        String bootstrapHost = args.length > argIndex ? args[argIndex] : WorkerServer.DEFAULT_HOST;
        int bootstrapPort = args.length > argIndex + 1
                ? Integer.parseInt(args[argIndex + 1])
                : BootstrapServer.DEFAULT_PORT;

        if (command.equals("stop")) {
            stopWorkers();
            return;
        }

        List<WorkerConfig> configs = loadConfig();
        System.out.println("Worker network configuration (" + configs.size() + " workers):");
        for (WorkerConfig c : configs) {
            System.out.println("  Worker " + c.id + " -> rmi://" + c.host + ":" + c.port
                    + "/" + WorkerServer.SERVICE_NAME_PREFIX + c.id);
        }

        List<Process> processes = startWorkers(configs, bootstrapHost, bootstrapPort);
        wireNeighbours(configs);
        triggerElection(configs);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> destroyAll(processes)));
        System.out.println("Launcher is supervising " + processes.size()
                + " worker processes. Press Ctrl+C to stop all workers.");
        while (true) {
            Thread.sleep(1000);
        }
    }

    private static List<WorkerConfig> loadConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream in = WorkerLauncher.class.getClassLoader().getResourceAsStream(CONFIG_RESOURCE)) {
            if (in == null) {
                throw new IOException("Missing classpath resource: " + CONFIG_RESOURCE);
            }
            props.load(in);
        }

        List<WorkerConfig> configs = new ArrayList<>();
        boolean more = true;
        for (int i = 1; more; i++) {
            String idKey = "worker." + i + ".id";
            String portKey = "worker." + i + ".port";
            if (!props.containsKey(idKey) || !props.containsKey(portKey)) {
                more = false;
                continue;
            }
            WorkerConfig c = new WorkerConfig();
            c.id = Integer.parseInt(props.getProperty(idKey));
            c.port = Integer.parseInt(props.getProperty(portKey));
            c.host = props.getProperty("worker." + i + ".host", WorkerServer.DEFAULT_HOST);
            configs.add(c);
        }

        if (configs.isEmpty()) {
            throw new IllegalStateException("No workers configured in " + CONFIG_RESOURCE);
        }
        return configs;
    }

    private static List<Process> startWorkers(List<WorkerConfig> configs,
                                              String bootstrapHost,
                                              int bootstrapPort) throws Exception {
        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = resolveClasspath();
        Path logDir = Path.of(DEFAULT_LOG_DIR);
        Files.createDirectories(logDir);

        List<Process> processes = new ArrayList<>();
        for (WorkerConfig c : configs) {
            List<String> cmd = List.of(
                    javaBin,
                    "-cp", classpath,
                    WorkerServer.class.getName(),
                    String.valueOf(c.id),
                    String.valueOf(c.port),
                    bootstrapHost,
                    String.valueOf(bootstrapPort),
                    c.host);

            ProcessBuilder pb = new ProcessBuilder(cmd);
            Path logFile = logDir.resolve("worker-" + c.id + ".log");
            pb.redirectOutput(logFile.toFile());
            pb.redirectErrorStream(true);

            Process process = pb.start();
            Files.writeString(logDir.resolve("worker-" + c.id + ".pid"), String.valueOf(process.pid()));
            processes.add(process);

            System.out.println("Started Worker Node " + c.id + " [process " + process.pid()
                    + ", log " + logFile + "]");

            Thread monitor = new Thread(() -> waitForWorker(c, process), "worker-monitor-" + c.id);
            monitor.setDaemon(true);
            monitor.start();
        }
        return processes;
    }

    private static String resolveClasspath() {
        String override = System.getProperty("worker.launcher.classpath");
        if (override != null && !override.isBlank()) {
            return override;
        }
        try {
            Path location = Path.of(WorkerLauncher.class.getProtectionDomain()
                    .getCodeSource().getLocation().toURI());
            return location.toString();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot determine the project classpath "
                    + "(pass -Dworker.launcher.classpath=<cp> to override)", e);
        }
    }

    private static void wireNeighbours(List<WorkerConfig> configs) throws Exception {
        System.out.println("Wiring the worker neighbour ring over RMI...");
        Thread.sleep(WIRE_DELAY_MS);

        int n = configs.size();
        for (int i = 0; i < n; i++) {
            WorkerConfig current = configs.get(i);
            WorkerConfig previous = configs.get((i - 1 + n) % n);
            WorkerConfig next = configs.get((i + 1) % n);

            Registry registry = LocateRegistry.getRegistry(current.host, current.port);
            WorkerService worker = (WorkerService) registry.lookup(
                    WorkerServer.SERVICE_NAME_PREFIX + current.id);
            worker.addNeighbour(previous.id);
            worker.addNeighbour(next.id);
            System.out.println("  Worker " + current.id + " neighbours: " + previous.id + ", " + next.id);
        }
    }

    private static void triggerElection(List<WorkerConfig> configs) throws Exception {
        System.out.println("Triggering leader election over the worker network...");
        WorkerConfig first = configs.get(0);
        Registry registry = LocateRegistry.getRegistry(first.host, first.port);
        WorkerService worker = (WorkerService) registry.lookup(
                WorkerServer.SERVICE_NAME_PREFIX + first.id);

        long electionId = worker.startElection();
        System.out.println("Election initiated from Worker " + first.id + " (electionId=" + electionId + ")");

        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline && worker.isElectionInProgress()) {
            Thread.sleep(400);
        }

        List<WorkerState> participants = worker.getLastElectionParticipants();
        System.out.println("Election completed. Coordinator: Worker " + worker.getCurrentCoordinatorId());
        System.out.println("Reachable participants (" + participants.size() + "):");
        participants.stream()
                .sorted(Comparator.comparingInt(WorkerState::getWorkerId))
                .forEach(state -> System.out.println("  Worker " + state.getWorkerId()
                        + " (JAC=" + state.getJac() + ") at " + state.getHost() + ":" + state.getPort()));
    }

    private static void waitForWorker(WorkerConfig c, Process process) {
        try {
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                System.err.println("Worker Node " + c.id + " finished prematurely with exit code "
                        + exitCode + " (see its log)");
            } else {
                System.out.println("Worker Node " + c.id + " stopped cleanly");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void destroyAll(List<Process> processes) {
        for (Process p : processes) {
            p.destroy();
        }
        for (Process p : processes) {
            try {
                p.waitFor(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("All worker processes stopped.");
    }

    private static void stopWorkers() {
        Path logDir = Path.of(DEFAULT_LOG_DIR);
        if (!Files.isDirectory(logDir)) {
            System.out.println("No " + DEFAULT_LOG_DIR + " directory found; nothing to stop.");
            return;
        }

        try (var paths = Files.list(logDir)) {
            List<Path> pidFiles = paths
                    .filter(p -> p.getFileName().toString().matches("worker-\\d+\\.pid"))
                    .sorted()
                    .toList();
            if (pidFiles.isEmpty()) {
                System.out.println("No worker PID files found in " + DEFAULT_LOG_DIR + "; nothing to stop.");
                return;
            }

            for (Path pidFile : pidFiles) {
                long pid = Long.parseLong(Files.readString(pidFile).trim());
                ProcessHandle.of(pid).ifPresentOrElse(
                        ph -> {
                            System.out.println("Stopping process " + pid + " (" + pidFile.getFileName() + ")");
                            ph.destroy();
                        },
                        () -> System.out.println("Process " + pid + " (" + pidFile.getFileName()
                                + ") is not running"));
                Files.deleteIfExists(pidFile);
            }
        } catch (Exception e) {
            System.err.println("Failed to stop workers: " + e.getMessage());
        }
    }
}