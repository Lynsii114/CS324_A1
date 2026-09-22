package com.cs324.frontend.client;

import com.cs324.backend.api.BootstrapService;
import com.cs324.backend.api.WorkerInfo;
import com.cs324.backend.bootstrap.BootstrapServer;
import com.cs324.backend.worker.WorkerClusterConfig;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

/**
 * Manual test harness that verifies the six-worker cluster.
 * Asks the Bootstrap Node which workers registered, then looks each one up
 * over its own RMI registry.
 */
public class ClusterStatusClient {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : WorkerClusterConfig.DEFAULT_HOST;
        int bootstrapPort = args.length > 1 ? Integer.parseInt(args[1]) : BootstrapServer.DEFAULT_PORT;

        Registry bootstrapRegistry = LocateRegistry.getRegistry(host, bootstrapPort);
        BootstrapService bootstrap = (BootstrapService) bootstrapRegistry.lookup(BootstrapServer.SERVICE_NAME);

        List<WorkerInfo> active = bootstrap.getActiveWorkers();
        System.out.println("Active workers reported by Bootstrap Node: " + active);

        int reachable = 0;
        for (int workerId : WorkerClusterConfig.workerIds()) {
            try {
                Registry workerRegistry = LocateRegistry.getRegistry(host, WorkerClusterConfig.portFor(workerId));
                com.cs324.backend.api.WorkerService worker =
                        (com.cs324.backend.api.WorkerService) workerRegistry.lookup(WorkerClusterConfig.serviceNameFor(workerId));
                System.out.println("Worker " + workerId + " OK: rmi://" + host + ":"
                        + WorkerClusterConfig.portFor(workerId) + "/" + WorkerClusterConfig.serviceNameFor(workerId)
                        + " | JAC=" + worker.getJobAllocationCounter()
                        + ", coordinator=" + worker.getCurrentCoordinatorId()
                        + ", neighbours=" + worker.getNeighbours()
                        + ", leaderman=" + worker.getLeaderman());
                reachable++;
            } catch (Exception e) {
                System.out.println("Worker " + workerId + " NOT reachable: " + e.getMessage());
            }
        }

        System.out.println(reachable + "/" + WorkerClusterConfig.WORKER_COUNT + " workers reachable over RMI");
        if (reachable != WorkerClusterConfig.WORKER_COUNT) {
            System.exit(1);
        }
    }
}
