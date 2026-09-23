package com.cs324.frontend.client;

import com.cs324.backend.api.BootstrapService;
import com.cs324.backend.api.WorkerInfo;
import com.cs324.backend.api.WorkerService;
import com.cs324.backend.bootstrap.BootstrapServer;
import com.cs324.backend.worker.WorkerClusterConfig;
import com.cs324.backend.worker.WorkerServer;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Manual test harness for the six-worker cluster.
 *
 * Subcommands:
 *   status                              show registration + per-worker state
 *   election [initiatorId]              run a leader election from a worker
 *   reset                               clear coordinators so a new election can run
 *   bump-jac <workerId> <amount>        increase a worker's JAC to test tie-breaking
 */
public class ClusterStatusClient {

    private static final String DEFAULT_HOST = "localhost";

    public static void main(String[] args) throws Exception {
        String command = args.length > 0 ? args[0] : "status";
        switch (command) {
            case "status", "list" -> status(host(args, 1), port(args, 2));
            case "election" -> election(args);
            case "reset" -> reset(host(args, 1), port(args, 2));
            case "bump-jac" -> bumpJac(args);
            default -> {
                System.err.println("Unknown command: " + command);
                System.err.println("Usage:");
                System.err.println("  ClusterStatusClient status [host] [bootstrapPort]");
                System.err.println("  ClusterStatusClient election [initiatorId] [host] [bootstrapPort]");
                System.err.println("  ClusterStatusClient reset [host] [bootstrapPort]");
                System.err.println("  ClusterStatusClient bump-jac <workerId> <amount> [host] [bootstrapPort]");
                System.exit(1);
            }
        }
    }

    private static void status(String host, int bootstrapPort) throws Exception {
        Registry bootstrapRegistry = LocateRegistry.getRegistry(host, bootstrapPort);
        BootstrapService bootstrap = (BootstrapService) bootstrapRegistry.lookup(BootstrapServer.SERVICE_NAME);

        List<WorkerInfo> active = bootstrap.getActiveWorkers();
        System.out.println("Active workers reported by Bootstrap Node: " + active);

        int reachable = 0;
        Set<Integer> coordinators = new TreeSet<>();
        for (int workerId : WorkerClusterConfig.workerIds()) {
            try {
                WorkerService worker = lookupWorker(host, workerId);
                int coordinator = worker.getCurrentCoordinatorId();
                coordinators.add(coordinator);
                System.out.println("Worker " + workerId + " OK: JAC=" + worker.getJobAllocationCounter()
                        + ", jobsThisTerm=" + worker.getJobsThisTerm()
                        + ", coordinator=" + formatCoordinator(coordinator)
                        + ", neighbours=" + worker.getNeighbours().size()
                        + ", leaderman=" + worker.getLeaderman());
                reachable++;
            } catch (Exception e) {
                System.out.println("Worker " + workerId + " NOT reachable: " + e.getMessage());
            }
        }

        System.out.println(reachable + "/" + WorkerClusterConfig.WORKER_COUNT + " workers reachable over RMI");
        if (reachable == WorkerClusterConfig.WORKER_COUNT) {
            if (coordinators.size() == 1 && !coordinators.contains(WorkerService.NO_COORDINATOR)) {
                System.out.println("Cluster is coordinated by worker " + coordinators.iterator().next());
            } else if (coordinators.contains(WorkerService.NO_COORDINATOR)) {
                System.out.println("No coordinator elected yet - run: ClusterStatusClient election [initiatorId]");
            } else {
                System.out.println("Coordinator mismatch across workers: " + coordinators
                        + " - reset and run a new election");
            }
        }
        System.exit(reachable == WorkerClusterConfig.WORKER_COUNT ? 0 : 1);
    }

    private static void election(String[] args) throws Exception {
        String host = host(args, 2);
        int bootstrapPort = port(args, 3);
        int initiatorId = args.length > 1 ? Integer.parseInt(args[1]) : 1;

        WorkerService initiator = lookupWorker(host, initiatorId);
        System.out.println("Initiating election from worker " + initiatorId);
        String result = initiator.initiateElection();
        System.out.println("Election result: " + result);
        System.out.println();
        status(host, bootstrapPort);
    }

    private static void reset(String host, int bootstrapPort) throws Exception {
        for (int workerId : WorkerClusterConfig.workerIds()) {
            WorkerService worker = lookupWorker(host, workerId);
            worker.setCurrentCoordinatorId(WorkerService.NO_COORDINATOR);
            System.out.println("Worker " + workerId + " coordinator reset to " + WorkerService.NO_COORDINATOR);
        }
        System.out.println();
        status(host, bootstrapPort);
    }

    private static void bumpJac(String[] args) throws Exception {
        String host = host(args, 3);
        int bootstrapPort = port(args, 4);
        int workerId = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        int amount = args.length > 2 ? Integer.parseInt(args[2]) : 1;

        WorkerService worker = lookupWorker(host, workerId);
        int jac = 0;
        for (int i = 0; i < amount; i++) {
            jac = worker.incrementJobAllocationCounter();
        }
        System.out.println("Worker " + workerId + " JAC is now " + jac);
    }

    private static WorkerService lookupWorker(String host, int workerId) throws Exception {
        Registry registry = LocateRegistry.getRegistry(host, WorkerClusterConfig.portFor(workerId));
        return (WorkerService) registry.lookup(WorkerServer.SERVICE_NAME_PREFIX + workerId);
    }

    private static String host(String[] args, int index) {
        return args.length > index ? args[index] : DEFAULT_HOST;
    }

    private static int port(String[] args, int index) {
        return args.length > index ? Integer.parseInt(args[index]) : BootstrapServer.DEFAULT_PORT;
    }

    private static String formatCoordinator(int coordinatorId) {
        return coordinatorId == WorkerService.NO_COORDINATOR ? "none" : String.valueOf(coordinatorId);
    }
}