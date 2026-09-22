package com.cs324.bootstrap;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

/**
 * Manual test harness for the Bootstrap Node. Not part of the Bootstrap Node
 * process itself - simulates workers registering/unregistering to verify RMI wiring.
 */
public class BootstrapClientTest {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : BootstrapServer.DEFAULT_PORT;

        Registry registry = LocateRegistry.getRegistry(host, port);
        BootstrapService bootstrap = (BootstrapService) registry.lookup(BootstrapServer.SERVICE_NAME);

        bootstrap.registerWorker(new WorkerInfo("worker-1", "localhost", 5001));
        bootstrap.registerWorker(new WorkerInfo("worker-2", "localhost", 5002));
        bootstrap.registerWorker(new WorkerInfo("worker-3", "localhost", 5003));

        List<WorkerInfo> active = bootstrap.getActiveWorkers();
        System.out.println("Active workers: " + active);

        WorkerInfo random = bootstrap.getRandomWorker();
        System.out.println("Randomly selected worker: " + random);

        bootstrap.unregisterWorker("worker-2");
        System.out.println("Active workers after unregistering worker-2: " + bootstrap.getActiveWorkers());
    }
}
