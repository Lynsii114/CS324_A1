package com.cs324.worker;

import com.cs324.bootstrap.BootstrapService;
import com.cs324.bootstrap.WorkerInfo;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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
    private final Map<UUID, ElectionCandidate> bestElectionCandidates = new ConcurrentHashMap<>();
    private final AtomicInteger currentCoordinatorId;
    private final AtomicInteger currentCoordinatorJac;
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
        this.currentCoordinatorJac = new AtomicInteger(jobAllocationCounter.get());
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
            System.out.println("[Worker " + this.workerId + "] NEIGHBOUR connected -> workerId="
                    + workerId + ", neighbours=" + getNeighbours());
        }
    }

    @Override
    public void removeNeighbour(int workerId) throws RemoteException {
        neighbours.remove(workerId);
        System.out.println("[Worker " + this.workerId + "] NEIGHBOUR removed -> workerId="
                + workerId + ", neighbours=" + getNeighbours());
    }

    @Override
    public int getCurrentCoordinatorId() throws RemoteException {
        return currentCoordinatorId.get();
    }

    @Override
    public void setCurrentCoordinatorId(int coordinatorId) throws RemoteException {
        currentCoordinatorId.set(coordinatorId);
        currentCoordinatorJac.set(0);
        System.out.println("[Worker " + workerId + "] Coordinator manually set -> coordinatorId="
                + coordinatorId);
    }

    @Override
    public void startElection() throws RemoteException {
        ElectionMessage message = new ElectionMessage(workerId, jobAllocationCounter.get());
        System.out.println("[Worker " + workerId + "] ELECTION started -> workerId=" + workerId
                + ", JAC=" + jobAllocationCounter.get() + ", messageId=" + message.getMessageId());
        receiveElectionMessage(message);
    }

    @Override
    public void receiveElectionMessage(ElectionMessage message) throws RemoteException {
        if (message == null) {
            return;
        }

        ElectionCandidate incoming = new ElectionCandidate(
                message.getCandidateWorkerId(), message.getCandidateJac());
        ElectionCandidate local = new ElectionCandidate(workerId, jobAllocationCounter.get());
        ElectionCandidate previousBest = bestElectionCandidates.get(message.getMessageId());
        ElectionCandidate selected = bestCandidate(previousBest, bestCandidate(incoming, local));

        if (selected.equals(previousBest)) {
            System.out.println("[Worker " + workerId + "] ELECTION ignored duplicate/stale -> "
                    + message + ", knownBest=" + previousBest);
            return;
        }

        bestElectionCandidates.put(message.getMessageId(), selected);
        System.out.println("[Worker " + workerId + "] ELECTION received -> incomingWorkerId="
                + incoming.workerId + ", incomingJAC=" + incoming.jac
                + ", localWorkerId=" + workerId + ", localJAC=" + local.jac
                + ", selectedCoordinator=" + selected.workerId
                + ", selectedJAC=" + selected.jac
                + ", tieBreak=highestWorkerId");

        ElectionMessage selectedMessage = new ElectionMessage(
                message.getMessageId(), selected.workerId, selected.jac);
        forwardElectionMessage(selectedMessage);
        broadcastCoordinator(selected.workerId, selected.jac);
    }

    @Override
    public void broadcastCoordinator(int coordinatorId) throws RemoteException {
        broadcastCoordinator(coordinatorId, 0);
    }

    private void broadcastCoordinator(int coordinatorId, int coordinatorJac) throws RemoteException {
        CoordinatorMessage message = new CoordinatorMessage(coordinatorId, coordinatorJac);
        System.out.println("[Worker " + workerId + "] COORDINATOR broadcast -> coordinatorId="
                + coordinatorId + ", coordinatorJAC=" + coordinatorJac
                + ", messageId=" + message.getMessageId());
        receiveCoordinatorMessage(message);
    }

    @Override
    public void receiveCoordinatorMessage(CoordinatorMessage message) throws RemoteException {
        if (message == null || !processedCoordinatorMessages.add(message.getMessageId())) {
            if (message != null) {
                System.out.println("[Worker " + workerId + "] COORDINATOR ignored duplicate -> "
                        + message);
            }
            return;
        }

        ElectionCandidate incoming = new ElectionCandidate(
                message.getCoordinatorId(), message.getCoordinatorJac());
        ElectionCandidate current = new ElectionCandidate(
                currentCoordinatorId.get(), currentCoordinatorJac.get());
        if (!incoming.equals(bestCandidate(current, incoming))) {
            System.out.println("[Worker " + workerId + "] COORDINATOR ignored stale -> "
                    + message + ", currentCoordinatorId=" + current.workerId
                    + ", currentCoordinatorJAC=" + current.jac);
            return;
        }

        currentCoordinatorId.set(message.getCoordinatorId());
        currentCoordinatorJac.set(message.getCoordinatorJac());
        System.out.println("[Worker " + workerId + "] COORDINATOR received -> coordinatorId="
                + message.getCoordinatorId() + ", messageId=" + message.getMessageId()
                + ", currentCoordinatorId=" + currentCoordinatorId.get()
                + ", currentCoordinatorJAC=" + currentCoordinatorJac.get());
        forwardCoordinatorMessage(message);
    }

    @Override
    public int submitMaxJob(List<Integer> numbers) throws RemoteException {
        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be null or empty");
        }
        if (currentCoordinatorId.get() != workerId) {
            throw new RemoteException("Worker " + workerId
                    + " is not the coordinator. Current coordinator is " + currentCoordinatorId.get());
        }

        List<WorkerService> reachableWorkers = getReachableWorkerServices();
        int workerCount = Math.min(reachableWorkers.size(), numbers.size());
        if (workerCount == 0) {
            throw new RemoteException("No reachable workers are available for MAX job");
        }

        System.out.println("[Worker " + workerId + "] MAX job received -> numbers="
                + numbers + ", reachableWorkers=" + reachableWorkers.size()
                + ", assignedWorkers=" + workerCount);

        int finalMax = Integer.MIN_VALUE;
        int start = 0;
        for (int index = 0; index < workerCount; index++) {
            int remainingNumbers = numbers.size() - start;
            int remainingWorkers = workerCount - index;
            int chunkSize = (remainingNumbers + remainingWorkers - 1) / remainingWorkers;
            List<Integer> chunk = new ArrayList<>(numbers.subList(start, start + chunkSize));
            WorkerService worker = reachableWorkers.get(index);
            int assignedWorkerId = worker.getWorkerId();

            try {
                System.out.println("[Worker " + workerId + "] MAX assigning -> toWorkerId="
                        + assignedWorkerId + ", section=" + chunk);
                int partialMax = worker.computePartialMax(chunk);
                finalMax = Math.max(finalMax, partialMax);
                System.out.println("[Worker " + workerId + "] MAX partial result <- fromWorkerId="
                        + assignedWorkerId + ", partialMax=" + partialMax
                        + ", currentFinalMax=" + finalMax);
            } catch (Exception e) {
                throw new RemoteException("MAX job failed while assigning worker "
                        + assignedWorkerId, e);
            }

            start += chunkSize;
        }

        System.out.println("[Worker " + workerId + "] MAX final result -> max=" + finalMax);
        return finalMax;
    }

    @Override
    public int computePartialMax(List<Integer> numbers) throws RemoteException {
        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be null or empty");
        }

int partialMax = Collections.max(numbers);
        int updatedJac = jobAllocationCounter.incrementAndGet();
        if (currentCoordinatorId.get() == workerId) {
            currentCoordinatorJac.set(updatedJac);
        }
        System.out.println("[Worker " + workerId + "] MAX partial compute -> section="
                + numbers + ", partialMax=" + partialMax + ", JAC=" + updatedJac);
        return partialMax;
    }

    private List<WorkerService> getReachableWorkerServices() throws RemoteException {
        if (bootstrap == null) {
            return Collections.singletonList(this);
        }

        Map<Integer, WorkerInfo> activeWorkers = new HashMap<>();
        for (WorkerInfo worker : bootstrap.getActiveWorkers()) {
            activeWorkers.put(worker.getWorkerId(), worker);
        }

        List<Integer> reachableWorkerIds = new ArrayList<>();
        Set<Integer> visited = new HashSet<>();
        Queue<Integer> pending = new ArrayDeque<>();
        pending.add(workerId);
        visited.add(workerId);

        while (!pending.isEmpty()) {
            int nextWorkerId = pending.remove();
            WorkerService service = lookupReachableWorker(activeWorkers, nextWorkerId);
            if (service == null) {
                continue;
            }

            reachableWorkerIds.add(nextWorkerId);
            try {
                for (Integer neighbourId : service.getNeighbours()) {
                    if (activeWorkers.containsKey(neighbourId) && visited.add(neighbourId)) {
                        pending.add(neighbourId);
                    }
                }
            } catch (RemoteException e) {
                System.err.println("[Worker " + workerId + "] Could not inspect neighbours for worker "
                        + nextWorkerId + ": " + e.getMessage());
            }
        }

        Collections.sort(reachableWorkerIds);
        List<WorkerService> services = new ArrayList<>();
        for (Integer reachableWorkerId : reachableWorkerIds) {
            WorkerService service = lookupReachableWorker(activeWorkers, reachableWorkerId);
            if (service != null) {
                services.add(service);
            }
        }
        return services;
    }

    private WorkerService lookupReachableWorker(Map<Integer, WorkerInfo> activeWorkers, int workerId) {
        if (workerId == this.workerId) {
            return this;
        }

        WorkerInfo worker = activeWorkers.get(workerId);
        if (worker == null) {
            return null;
        }

        try {
            return lookupWorker(worker);
        } catch (Exception e) {
            System.err.println("[Worker " + this.workerId + "] Worker " + workerId
                    + " is not reachable: " + e.getMessage());
            return null;
        }
    }

    private void forwardElectionMessage(ElectionMessage message) {
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
                    System.out.println("[Worker " + workerId + "] ELECTION forwarding -> toWorkerId="
                            + neighbourId + ", candidateWorkerId=" + message.getCandidateWorkerId()
                            + ", candidateJAC=" + message.getCandidateJac()
                            + ", messageId=" + message.getMessageId());
                    WorkerService neighbourService = lookupWorker(neighbour);
                    neighbourService.receiveElectionMessage(message);
                } catch (Exception e) {
                    System.err.println("Could not forward election message to worker "
                            + neighbourId + ": " + e.getMessage());
                }
            }
        } catch (RemoteException e) {
            System.err.println("Could not resolve worker neighbours: " + e.getMessage());
        }
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
                    System.out.println("[Worker " + workerId + "] COORDINATOR forwarding -> toWorkerId="
                            + neighbourId + ", coordinatorId=" + message.getCoordinatorId()
                            + ", coordinatorJAC=" + message.getCoordinatorJac()
                            + ", messageId=" + message.getMessageId());
                    WorkerService neighbourService = lookupWorker(neighbour);
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

    private WorkerService lookupWorker(WorkerInfo worker) throws Exception {
        Registry registry = LocateRegistry.getRegistry(worker.getHost(), worker.getPort());
        return (WorkerService) registry.lookup(WorkerServer.SERVICE_NAME_PREFIX + worker.getWorkerId());
    }

    private ElectionCandidate bestCandidate(ElectionCandidate first, ElectionCandidate second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        if (second.jac > first.jac) {
            return second;
        }
        if (second.jac == first.jac && second.workerId > first.workerId) {
            return second;
        }
        return first;
    }

    private static final class ElectionCandidate {
        private final int workerId;
        private final int jac;

        private ElectionCandidate(int workerId, int jac) {
            this.workerId = workerId;
            this.jac = jac;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ElectionCandidate)) {
                return false;
            }
            ElectionCandidate other = (ElectionCandidate) o;
            return workerId == other.workerId && jac == other.jac;
        }

        @Override
        public int hashCode() {
            return 31 * workerId + jac;
        }

        @Override
        public String toString() {
            return "ElectionCandidate{workerId=" + workerId + ", jac=" + jac + '}';
        }
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
