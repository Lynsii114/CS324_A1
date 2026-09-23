package com.cs324.frontend.client;

import com.cs324.backend.api.WorkerService;
import com.cs324.backend.worker.WorkerServer;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Manual PRIMESUM client. Send the job to the current coordinator worker.
 */
public class PrimeSumClientTest {
    private static final String DEFAULT_HOST = WorkerServer.DEFAULT_HOST;
    private static final int DEFAULT_COORDINATOR_PORT = 5006;
    private static final int DEFAULT_COORDINATOR_ID = 6;
    private static final int DEFAULT_START = 1;
    private static final int DEFAULT_END = 600;

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : DEFAULT_HOST;
        int coordinatorPort = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_COORDINATOR_PORT;
        int coordinatorId = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_COORDINATOR_ID;
        int start = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_START;
        int end = args.length > 4 ? Integer.parseInt(args[4]) : DEFAULT_END;

        Registry registry = LocateRegistry.getRegistry(host, coordinatorPort);
        WorkerService coordinator = (WorkerService) registry.lookup(
                WorkerServer.SERVICE_NAME_PREFIX + coordinatorId);

        System.out.println("[Client] Sending PRIMESUM job -> coordinatorId=" + coordinator.getWorkerId()
                + ", range=[" + start + ", " + end + "]");
        long result = coordinator.submitPrimeSum(start, end);
        System.out.println("[Client] PRIMESUM result <- " + result);
    }
}