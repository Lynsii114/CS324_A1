package com.cs324.worker;

import com.cs324.bootstrap.BootstrapService;
import com.cs324.bootstrap.WorkerInfo;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

/** Coordinates a PRIMECOUNT request across the currently registered workers. */
public final class PrimeCountCoordinator {
    private PrimeCountCoordinator() { }

    /**
     * Divides the input into contiguous sections whose sizes differ by at most
     * one, sends one section to each active worker, and sums the partial counts.
     */
    public static int primeCount(List<Integer> numbers, BootstrapService bootstrap) throws Exception {
        if (numbers == null) throw new IllegalArgumentException("numbers must not be null");
        if (bootstrap == null) throw new IllegalArgumentException("bootstrap must not be null");

        List<WorkerInfo> workers = bootstrap.getActiveWorkers();
        if (workers == null || workers.isEmpty()) {
            throw new IllegalStateException("No active workers are available");
        }

        int workerCount = workers.size();
        // The first active worker coordinates this request. Its own local
        // section is not an allocation; each remote section is one assignment.
        WorkerInfo coordinatorInfo = workers.get(0);
        Registry coordinatorRegistry = LocateRegistry.getRegistry(
                coordinatorInfo.getHost(), coordinatorInfo.getPort());
        WorkerService coordinator = (WorkerService) coordinatorRegistry.lookup(
                WorkerServer.SERVICE_NAME_PREFIX + coordinatorInfo.getWorkerId());
        int baseSize = numbers.size() / workerCount;
        int remainder = numbers.size() % workerCount;
        int offset = 0;
        int total = 0;
        for (int i = 0; i < workerCount; i++) {
            int sectionSize = baseSize + (i < remainder ? 1 : 0);
            List<Integer> section = new ArrayList<>(numbers.subList(offset, offset + sectionSize));
            offset += sectionSize;

            WorkerInfo info = workers.get(i);
            if (info.getWorkerId() != coordinatorInfo.getWorkerId()) {
                coordinator.recordJobAllocation();
            }
            Registry registry = LocateRegistry.getRegistry(info.getHost(), info.getPort());
            WorkerService worker = (WorkerService) registry.lookup(
                    WorkerServer.SERVICE_NAME_PREFIX + info.getWorkerId());
            total += worker.countPrimes(section);
        }
        return total;
    }

    /** Connects to the Bootstrap Node and runs PRIMECOUNT from a client process. */
    public static int primeCount(List<Integer> numbers, String bootstrapHost, int bootstrapPort)
            throws Exception {
        Registry registry = LocateRegistry.getRegistry(bootstrapHost, bootstrapPort);
        BootstrapService bootstrap = (BootstrapService) registry.lookup(
                com.cs324.bootstrap.BootstrapServer.SERVICE_NAME);
        return primeCount(numbers, bootstrap);
    }
}
