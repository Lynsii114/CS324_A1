package com.cs324.frontend.client;

import com.cs324.backend.api.WorkerService;
import com.cs324.backend.worker.WorkerServer;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Manual MAX job client. Send the job to the current coordinator worker.
 */
public class WorkerMaxClientTest {
    private static final String DEFAULT_HOST = WorkerServer.DEFAULT_HOST;
    private static final int DEFAULT_COORDINATOR_PORT = 5006;
    private static final int DEFAULT_COORDINATOR_ID = 6;
    private static final List<Integer> DEFAULT_NUMBERS = Arrays.asList(12, 7, 44, 3, 91, 18, 55, 6, 72, 10, 29);

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : DEFAULT_HOST;
        int coordinatorPort = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_COORDINATOR_PORT;
        int coordinatorId = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_COORDINATOR_ID;
        List<Integer> numbers = args.length > 3 ? parseNumbers(args[3]) : DEFAULT_NUMBERS;

        Registry registry = LocateRegistry.getRegistry(host, coordinatorPort);
        WorkerService coordinator = (WorkerService) registry.lookup(
                WorkerServer.SERVICE_NAME_PREFIX + coordinatorId);

        System.out.println("[Client] Sending MAX job -> coordinatorId=" + coordinator.getWorkerId()
                + ", numbers=" + numbers);
        int result = coordinator.submitMaxJob(numbers);
        System.out.println("[Client] MAX result <- " + result);
    }

    private static List<Integer> parseNumbers(String value) {
        List<Integer> numbers = new ArrayList<>();
        for (String part : value.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                numbers.add(Integer.parseInt(trimmed));
            }
        }
        return numbers;
    }
}
