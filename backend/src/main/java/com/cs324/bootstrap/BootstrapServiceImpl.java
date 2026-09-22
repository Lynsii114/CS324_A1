package com.cs324.bootstrap;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Thread-safe implementation of the Bootstrap Node. Keeps membership only;
 * no leader election or job dispatch logic lives here by design.
 */
public class BootstrapServiceImpl extends UnicastRemoteObject implements BootstrapService {
    private static final long serialVersionUID = 1L;

    // ConcurrentHashMap gives us thread-safe register/unregister/read without external locking.
    private final Map<String, WorkerInfo> activeWorkers = new ConcurrentHashMap<>();

    protected BootstrapServiceImpl() throws RemoteException {
        super();
    }

    @Override
    public void registerWorker(WorkerInfo worker) throws RemoteException {
        if (worker == null || worker.getWorkerId() == null) {
            throw new IllegalArgumentException("worker and worker id must not be null");
        }
        activeWorkers.put(worker.getWorkerId(), worker);
        System.out.println("[Bootstrap] Registered worker: " + worker);
    }

    @Override
    public void unregisterWorker(String workerId) throws RemoteException {
        WorkerInfo removed = activeWorkers.remove(workerId);
        if (removed != null) {
            System.out.println("[Bootstrap] Unregistered worker: " + removed);
        } else {
            System.out.println("[Bootstrap] Unregister requested for unknown worker id: " + workerId);
        }
    }

    @Override
    public List<WorkerInfo> getActiveWorkers() throws RemoteException {
        return new ArrayList<>(activeWorkers.values());
    }

    @Override
    public WorkerInfo getRandomWorker() throws RemoteException {
        List<WorkerInfo> workers = new ArrayList<>(activeWorkers.values());
        if (workers.isEmpty()) {
            return null;
        }
        int index = ThreadLocalRandom.current().nextInt(workers.size());
        return workers.get(index);
    }
}
