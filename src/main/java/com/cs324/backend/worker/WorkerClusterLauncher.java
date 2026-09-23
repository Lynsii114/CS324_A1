package com.cs324.backend.worker;

import com.cs324.backend.api.WorkerService;
import com.cs324.backend.bootstrap.BootstrapServer;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

/**
 * Launches the six configured Worker Nodes (IDs 1-6) as six independent JVM
 * processes. Each process gets its own RMI registry port, its own log file and
 * its own pid file, so the workers stay fully isolated while still talking to
 * each other (and to the Bootstrap Node) through Java RMI.
 *
 * Usage:
 *   WorkerClusterLauncher [start|stop|status] [bootstrapHost] [bootstrapPort]
 */
public final class WorkerClusterLauncher {

    private static final long LOOKUP_TIMEOUT_MS = 20_000;
    private static final long LOOKUP_POLL_MS = 500;

    private WorkerClusterLauncher() {
    }

    public static void main(String[] args) throws Exception {
        String command = args.length > 0 ? args[0] : "start";
        String bootstrapHost = args.length > 1 ? args[1] : WorkerClusterConfig.DEFAULT_HOST;
        int bootstrapPort = args.length > 2 ? Integer.parseInt(args[2]) : BootstrapServer.DEFAULT_PORT;

        switch (command) {
            case "start" -> start(bootstrapHost, bootstrapPort);
            case "stop" -> stop();
            case "status" -> status();
            default -> {
                System.err.println("Unknown command: " + command);
                System.err.println("Usage: WorkerClusterLauncher [start|stop|status] [bootstrapHost] [bootstrapPort]");
                System.exit(1);
            }
        }
    }

    private static void start(String bootstrapHost, int bootstrapPort) throws Exception {
        Path logDir = Paths.get(WorkerClusterConfig.LOG_DIR);
        Files.createDirectories(logDir);

        String classpath = currentClasspath();
        String javaBin = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
        Path workingDir = Paths.get("").toAbsolutePath();

        List<Integer> started = new ArrayList<>();
        List<Integer> alreadyRunning = new ArrayList<>();

        for (int workerId : WorkerClusterConfig.workerIds()) {
            Path pidFile = logDir.resolve(pidFileName(workerId));
            Integer existingPid = readPid(pidFile);
            if (existingPid != null && isAlive(existingPid)) {
                alreadyRunning.add(workerId);
                System.out.println("Worker " + workerId + " already running with PID " + existingPid);
                continue;
            }

            int port = WorkerClusterConfig.portFor(workerId);
            ProcessBuilder builder = new ProcessBuilder(
                    javaBin,
                    "-cp", classpath,
                    WorkerServer.class.getName(),
                    String.valueOf(workerId),
                    String.valueOf(port),
                    bootstrapHost,
                    String.valueOf(bootstrapPort),
                    WorkerClusterConfig.DEFAULT_HOST);
            builder.directory(workingDir.toFile());
            builder.redirectOutput(ProcessBuilder.Redirect.to(logDir.resolve(logFileName(workerId)).toFile()));
            builder.redirectError(ProcessBuilder.Redirect.appendTo(logDir.resolve(logFileName(workerId)).toFile()));

            Process process = builder.start();
            Files.writeString(pidFile, String.valueOf(process.pid()), StandardCharsets.UTF_8);
            started.add(workerId);
            System.out.println("Started Worker " + workerId + " (pid " + process.pid()
                    + ", rmi port " + port + ", log " + logDir.resolve(logFileName(workerId)) + ")");
        }

        if (!started.isEmpty()) {
            verifyOverRmi(started, WorkerClusterConfig.DEFAULT_HOST);
        }

        System.out.println();
        System.out.println("Cluster state: " + (started.size() + alreadyRunning.size())
                + "/" + WorkerClusterConfig.WORKER_COUNT + " workers running"
                + (alreadyRunning.isEmpty() ? "" : " (already running: " + alreadyRunning + ")"));
    }

    /** Polls each worker's own RMI registry until the worker is reachable. */
    private static void verifyOverRmi(List<Integer> workerIds, String host) {
        long deadline = System.currentTimeMillis() + LOOKUP_TIMEOUT_MS;
        List<Integer> pending = new ArrayList<>(workerIds);

        while (!pending.isEmpty() && System.currentTimeMillis() < deadline) {
            pending.removeIf(workerId -> {
                try {
                    Registry registry = LocateRegistry.getRegistry(host, WorkerClusterConfig.portFor(workerId));
                    WorkerService worker = (WorkerService) registry.lookup(WorkerClusterConfig.serviceNameFor(workerId));
                    return worker.getWorkerId() == workerId;
                } catch (Exception e) {
                    return false;
                }
            });
            if (!pending.isEmpty()) {
                try {
                    Thread.sleep(LOOKUP_POLL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        for (Integer workerId : workerIds) {
            if (pending.contains(workerId)) {
                System.err.println("Worker " + workerId + " did not answer its RMI lookup within "
                        + LOOKUP_TIMEOUT_MS + " ms - check its log file");
            } else {
                System.out.println("Verified over RMI: worker " + workerId
                        + " at rmi://" + host + ":" + WorkerClusterConfig.portFor(workerId)
                        + "/" + WorkerClusterConfig.serviceNameFor(workerId));
            }
        }
    }

    private static void stop() {
        int stopped = 0;
        for (int workerId : WorkerClusterConfig.workerIds()) {
            Path pidFile = Paths.get(WorkerClusterConfig.LOG_DIR, pidFileName(workerId));
            Integer pid = readPid(pidFile);
            if (pid == null || !isAlive(pid)) {
                System.out.println("Worker " + workerId + " is not running");
                deleteQuietly(pidFile);
                continue;
            }
            ProcessHandle.of(pid).ifPresent(handle -> {
                handle.destroy();
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (handle.isAlive()) {
                    handle.destroyForcibly();
                }
            });
            System.out.println("Stopped Worker " + workerId + " (pid " + pid + ")");
            deleteQuietly(pidFile);
            stopped++;
        }
        System.out.println("Stopped " + stopped + " worker process(es)");
    }

    private static void status() {
        int running = 0;
        for (int workerId : WorkerClusterConfig.workerIds()) {
            Path pidFile = Paths.get(WorkerClusterConfig.LOG_DIR, pidFileName(workerId));
            Integer pid = readPid(pidFile);
            boolean alive = pid != null && isAlive(pid);
            if (alive) {
                running++;
            }
            System.out.println("Worker " + workerId
                    + " (port " + WorkerClusterConfig.portFor(workerId) + "): "
                    + (alive ? "RUNNING, pid " + pid : "not running"));
        }
        System.out.println(running + "/" + WorkerClusterConfig.WORKER_COUNT + " workers running");
    }

    /**
     * Resolves the classpath of the running project. Works both under
     * {@code mvn exec:java} (isolated classloader) and under a plain
     * {@code java -cp target/classes} launch.
     */
    private static String currentClasspath() {
        try {
            URI uri = WorkerClusterLauncher.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            return Paths.get(uri).toAbsolutePath().toString();
        } catch (Exception e) {
            return System.getProperty("java.class.path");
        }
    }

    private static String logFileName(int workerId) {
        return "worker-" + workerId + ".log";
    }

    private static String pidFileName(int workerId) {
        return "worker-" + workerId + ".pid";
    }

    private static Integer readPid(Path pidFile) {
        try {
            if (!Files.exists(pidFile)) {
                return null;
            }
            String raw = Files.readString(pidFile, StandardCharsets.UTF_8).trim();
            return raw.isEmpty() ? null : Integer.valueOf(raw);
        } catch (IOException | NumberFormatException e) {
            return null;
        }
    }

    private static boolean isAlive(int pid) {
        return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // best effort cleanup
        }
    }
}
