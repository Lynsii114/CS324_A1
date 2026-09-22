package com.cs324.worker;

import com.cs324.bootstrap.BootstrapService;
import com.cs324.bootstrap.WorkerInfo;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Worker state holder and coordinator-message forwarder.
 */
public class WorkerServiceImpl extends UnicastRemoteObject implements WorkerService {
    private static final long serialVersionUID = 1L;

    private final int workerId;
    private final AtomicInteger jobAllocationCounter = new AtomicInteger(0);
    private final Set<Integer> neighbours = ConcurrentHashMap.newKeySet();
    private final Set<UUID> processedCoordinatorMessages = ConcurrentHashMap.newKeySet();
    private final AtomicInteger currentCoordinatorId;
    private final BootstrapService bootstrap;
    private final String leaderman = "cs324";

    public WorkerServiceImpl(int workerId) throws RemoteException {
        this(workerId, null);
    }

    public WorkerServiceImpl(int workerId, BootstrapService bootstrap) throws RemoteException {
        super();
        this.workerId = workerId;
        this.bootstrap = bootstrap;
        this.currentCoordinatorId = new AtomicInteger(workerId);
    }

    @Override
    public int getWorkerId() throws RemoteException {
        return workerId;
    }

    @Override
    public int getJobAllocationCounter() throws RemoteException {
        return jobAllocationCounter.get();
    }

    @Override
    public List<Integer> getNeighbours() throws RemoteException {
        return new ArrayList<>(neighbours);
    }

    @Override
    public void addNeighbour(int workerId) throws RemoteException {
        if (workerId != this.workerId) {
            neighbours.add(workerId);
        }
    }

    @Override
    public void removeNeighbour(int workerId) throws RemoteException {
        neighbours.remove(workerId);
    }

    @Override
    public int getCurrentCoordinatorId() throws RemoteException {
        return currentCoordinatorId.get();
    }

    @Override
    public void setCurrentCoordinatorId(int coordinatorId) throws RemoteException {
        currentCoordinatorId.set(coordinatorId);
    }

    @Override
    public void broadcastCoordinator(int coordinatorId) throws RemoteException {
        receiveCoordinatorMessage(new CoordinatorMessage(coordinatorId));
    }

    @Override
    public void receiveCoordinatorMessage(CoordinatorMessage message) throws RemoteException {
        if (message == null || !processedCoordinatorMessages.add(message.getMessageId())) {
            return;
        }

        currentCoordinatorId.set(message.getCoordinatorId());
        forwardCoordinatorMessage(message);
    }

    private void forwardCoordinatorMessage(CoordinatorMessage message) {
        if (bootstrap == null) {
            return;
        }

        try {
            List<WorkerInfo> activeWorkers = bootstrap.getActiveWorkers();
            for (Integer neighbourId : neighbours) {
                WorkerInfo neighbour = findWorker(activeWorkers, neighbourId);
                if (neighbour == null || neighbour.getWorkerId() == workerId) {
                    continue;
                }

                try {
                    Registry registry = LocateRegistry.getRegistry(neighbour.getHost(), neighbour.getPort());
                    WorkerService neighbourService = (WorkerService) registry.lookup(
                            WorkerServer.SERVICE_NAME_PREFIX + neighbour.getWorkerId());
                    neighbourService.receiveCoordinatorMessage(message);
                } catch (Exception e) {
                    System.err.println("Could not forward coordinator message to worker "
                            + neighbourId + ": " + e.getMessage());
                }
            }
        } catch (RemoteException e) {
            System.err.println("Could not resolve worker neighbours: " + e.getMessage());
        }
    }

    private WorkerInfo findWorker(List<WorkerInfo> activeWorkers, int workerId) {
        for (WorkerInfo worker : activeWorkers) {
            if (worker.getWorkerId() == workerId) {
                return worker;
            }
        }
        return null;
    }

    @Override
    public String getLeaderman() throws RemoteException {
        return leaderman;
    }

    @Override
    public String toString() {
        return "WorkerServiceImpl{"
                + "workerId=" + workerId
                + ", jobAllocationCounter=" + jobAllocationCounter.get()
                + ", neighbours=" + neighbours
                + ", currentCoordinatorId=" + currentCoordinatorId.get()
                + ", leaderman='" + leaderman + '\''
                + '}';
    }
}
