package com.cs324.backend.bootstrap;

import java.rmi.server.ExportException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Entry point that runs the Bootstrap Node as its own JVM process.
 * Starts (or reuses) an RMI registry and binds the BootstrapService under a fixed name.
 */
public class BootstrapServer {

    public static final String SERVICE_NAME = "BootstrapService";
    public static final int DEFAULT_PORT = 1099;

    public static void main(String[] args) {
        int port = DEFAULT_PORT;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }

        try {
            Registry registry;
            try {
                registry = LocateRegistry.createRegistry(port);
                System.out.println("Created RMI registry on port " + port);
            } catch (ExportException alreadyRunning) {
                registry = LocateRegistry.getRegistry(port);
                System.out.println("Using existing RMI registry on port " + port);
            }

            BootstrapServiceImpl service = new BootstrapServiceImpl();
            registry.rebind(SERVICE_NAME, service);

            System.out.println("Bootstrap Node started on port " + port);
            System.out.println("Bound as rmi://<host>:" + port + "/" + SERVICE_NAME);
            System.out.println("Waiting for workers to register...");
        } catch (Exception e) {
            System.err.println("Bootstrap Node failed to start: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
