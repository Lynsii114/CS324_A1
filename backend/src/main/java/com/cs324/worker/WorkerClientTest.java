package com.cs324.worker;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Manual test harness for inspecting a running Worker Node over RMI.
 */
public class WorkerClientTest {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : WorkerServer.DEFAULT_HOST;
        int workerPort = args.length > 1 ? Integer.parseInt(args[1]) : 5001;
        int workerId = args.length > 2 ? Integer.parseInt(args[2]) : 1;

        Registry registry = LocateRegistry.getRegistry(host, workerPort);
        WorkerService worker = (WorkerService) registry.lookup(WorkerServer.SERVICE_NAME_PREFIX + workerId);

        System.out.println("Worker ID: " + worker.getWorkerId());
        System.out.println("JAC: " + worker.getJobAllocationCounter());
        System.out.println("Coordinator ID: " + worker.getCurrentCoordinatorId());
        System.out.println("Leaderman: " + worker.getLeaderman());

        worker.addNeighbour(2);
        worker.addNeighbour(3);
        System.out.println("Neighbours after add: " + worker.getNeighbours());

        worker.removeNeighbour(2);
        System.out.println("Neighbours after remove: " + worker.getNeighbours());
    }
}
