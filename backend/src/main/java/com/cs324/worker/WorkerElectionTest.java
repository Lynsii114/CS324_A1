package com.cs324.worker;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

/**
 * Manual 6-worker election test harness.
 * It connects workers in a ring, starts election at worker 1, then prints final coordinator state.
 */
public class WorkerElectionTest {
    private static final String DEFAULT_HOST = WorkerServer.DEFAULT_HOST;
    private static final int FIRST_WORKER_PORT = 5001;
    private static final int WORKER_COUNT = 6;

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : DEFAULT_HOST;
        int firstPort = args.length > 1 ? Integer.parseInt(args[1]) : FIRST_WORKER_PORT;

        List<WorkerService> workers = new ArrayList<>();
        for (int workerId = 1; workerId <= WORKER_COUNT; workerId++) {
            int port = firstPort + workerId - 1;
            Registry registry = LocateRegistry.getRegistry(host, port);
            WorkerService worker = (WorkerService) registry.lookup(WorkerServer.SERVICE_NAME_PREFIX + workerId);
            workers.add(worker);
            System.out.println("[Test] Registered worker reachable -> workerId=" + worker.getWorkerId()
                    + ", port=" + port + ", JAC=" + worker.getJobAllocationCounter()
                    + ", coordinatorId=" + worker.getCurrentCoordinatorId());
        }

        System.out.println("[Test] Connecting workers in a ring: 1-2-3-4-5-6-1");
        for (int i = 0; i < WORKER_COUNT; i++) {
            WorkerService worker = workers.get(i);
            int previousWorkerId = i == 0 ? WORKER_COUNT : i;
            int nextWorkerId = i == WORKER_COUNT - 1 ? 1 : i + 2;
            worker.addNeighbour(previousWorkerId);
            worker.addNeighbour(nextWorkerId);
            System.out.println("[Test] Worker " + worker.getWorkerId()
                    + " neighbours=" + worker.getNeighbours());
        }

        System.out.println("[Test] Starting election at worker 1");
        workers.get(0).startElection();

        Thread.sleep(3000);

        System.out.println("[Test] Final coordinator view");
        for (WorkerService worker : workers) {
            System.out.println("[Test] Worker " + worker.getWorkerId()
                    + " JAC=" + worker.getJobAllocationCounter()
                    + " coordinatorId=" + worker.getCurrentCoordinatorId());
        }
    }
}
