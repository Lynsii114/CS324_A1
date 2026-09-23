package com.cs324.backend.worker;

import com.cs324.backend.api.BootstrapService;
import com.cs324.backend.api.CandidateInfo;
import com.cs324.backend.api.ElectionMessage;
import com.cs324.backend.api.ProcessedParticipants;
import com.cs324.backend.api.WinnerAnnouncement;
import com.cs324.backend.api.WorkerInfo;
import com.cs324.backend.api.WorkerService;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Worker Node that holds worker state and runs the distributed leader election
 * protocol.
 *
 * <p>Election algorithm (echo/termination aggregation):
 * <ul>
 *   <li>An ELECTION message carries a unique election id and a growing set of
 *       discovered participants. Each worker keeps a thread-safe set of
 *       processed election ids so no message is handled twice and message loops
 *       are cut.</li>
 *   <li>The initiator broadcasts over its neighbour ring. Every reachable,
 *       active worker adds its own {@link CandidateInfo} snapshot and re-sends
 *       the message to its neighbours (except the hop it came from), and echoes
 *       the merged participant set back. When all echoes return, every reachable
 *       worker has been discovered.</li>
 *   <li>The winner is the reachable worker with the lowest JAC; ties are broken
 *       by the highest worker id.</li>
 *   <li>The result is broadcast to all participants as a
 *       {@link WinnerAnnouncement}, so every worker records the same
 *       coordinator.</li>
 *   <li>The Bootstrap Node is never involved beyond looking up the active worker
 *       registry; it does not participate in the election.</li>
 * </ul>
 */
public class WorkerServiceImpl extends UnicastRemoteObject implements WorkerService {
    private static final long serialVersionUID = 1L;

    private final int workerId;
    private final String registeredHost;
    private final int registeredPort;
    private final BootstrapService bootstrapService;

    private final AtomicInteger jobAllocationCounter = new AtomicInteger(0);
    private final AtomicInteger currentCoordinatorId = new AtomicInteger(NO_COORDINATOR);
    private final Set<String> processedElectionIds = ConcurrentHashMap.newKeySet();
    private final Set<WorkerInfo> neighbours = ConcurrentHashMap.newKeySet();
    private final String leaderman = "cs324";

    public WorkerServiceImpl(int workerId, WorkerInfo self, BootstrapService bootstrapService)
            throws RemoteException {
        super();
        this.workerId = workerId;
        this.registeredHost = self.getHost();
        this.registeredPort = self.getPort();
        this.bootstrapService = bootstrapService;
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
    public int incrementJobAllocationCounter() throws RemoteException {
        return jobAllocationCounter.incrementAndGet();
    }

    @Override
    public int resetJobAllocationCounter() throws RemoteException {
        jobAllocationCounter.set(0);
        return jobAllocationCounter.get();
    }

    @Override
    public List<WorkerInfo> getNeighbours() throws RemoteException {
        return new ArrayList<>(neighbours);
    }

    @Override
    public void addNeighbour(WorkerInfo worker) throws RemoteException {
        if (worker == null || worker.getWorkerId() == workerId) {
            return;
        }
        neighbours.add(worker);
    }

    @Override
    public void addNeighbour(int workerId) throws RemoteException {
        if (workerId == this.workerId) {
            return;
        }
        List<WorkerInfo> active = bootstrapService.getActiveWorkers();
        for (WorkerInfo info : active) {
            if (info.getWorkerId() == workerId) {
                neighbours.add(info);
                return;
            }
        }
        System.err.println("[Worker " + this.workerId + "] cannot resolve worker " + workerId
                + " via Bootstrap Node - it is not registered");
    }

    @Override
    public void removeNeighbour(int workerId) throws RemoteException {
        neighbours.removeIf(worker -> worker.getWorkerId() == workerId);
    }

    @Override
    public int getCurrentCoordinatorId() throws RemoteException {
        return currentCoordinatorId.get();
    }

    @Override
    public void setCurrentCoordinatorId(int coordinatorId) throws RemoteException {
        currentCoordinatorId.set(coordinatorId);
        System.out.println("[Worker " + workerId + "] coordinator set to " + coordinatorId);
    }

    @Override
    public String getLeaderman() throws RemoteException {
        return leaderman;
    }

    @Override
    public int syncNeighbours() throws RemoteException {
        return refreshNeighbours();
    }

    @Override
    public String initiateElection() throws RemoteException {
        int existing = currentCoordinatorId.get();
        if (existing != NO_COORDINATOR) {
            return "Coordinator already present: worker " + existing
                    + " - no election started (reset coordinators to " + NO_COORDINATOR + " first)";
        }

        refreshNeighbours();

        CandidateInfo self = new CandidateInfo(workerId, jobAllocationCounter.get(), registeredHost, registeredPort);
        String electionId = UUID.randomUUID().toString();
        // The initiator marks its own election as processed so a message that
        // loops back to it is dropped instead of being handled twice.
        processedElectionIds.add(electionId);

        System.out.println("[Worker " + workerId + "] election " + electionId + " started, JAC=" + self.getJac()
                + ", neighbours=" + neighbours.size());

        ElectionMessage message = new ElectionMessage(electionId, self, self, new LinkedHashSet<>());
        ProcessedParticipants collected = propagateToNeighbours(message);

        Set<CandidateInfo> allParticipants = new LinkedHashSet<>(collected.getParticipants());
        allParticipants.add(self);

        CandidateInfo winner = selectWinner(allParticipants);
        currentCoordinatorId.set(winner.getWorkerId());

        System.out.println("[Worker " + workerId + "] election " + electionId + " winner: worker "
                + winner.getWorkerId() + " (JAC=" + winner.getJac() + ") across "
                + allParticipants.size() + " reachable workers");

        WinnerAnnouncement announcement = new WinnerAnnouncement(electionId, winner, allParticipants);
        broadcastWinner(announcement);

        return "Coordinator elected: worker " + winner.getWorkerId() + " (JAC=" + winner.getJac()
                + ") across " + allParticipants.size() + " reachable workers [electionId=" + electionId + "]";
    }

    @Override
    public ProcessedParticipants receiveElection(ElectionMessage message) throws RemoteException {
        if (message == null || message.getElectionId() == null) {
            return null;
        }
        if (!processedElectionIds.add(message.getElectionId())) {
            System.out.println("[Worker " + workerId + "] dropping duplicate election message "
                    + message.getElectionId() + " from worker " + message.getSender().getWorkerId());
            return null;
        }

        refreshNeighbours();

        CandidateInfo self = new CandidateInfo(workerId, jobAllocationCounter.get(), registeredHost, registeredPort);
        Set<CandidateInfo> participants = new LinkedHashSet<>(message.getParticipants());
        participants.add(self);

        ElectionMessage forwarded = new ElectionMessage(message.getElectionId(), message.getOriginator(), self, participants);
        return propagateToNeighbours(forwarded);
    }

    @Override
    public void announceWinner(WinnerAnnouncement announcement) throws RemoteException {
        if (announcement == null || announcement.getWinner() == null) {
            return;
        }
        processedElectionIds.add(announcement.getElectionId());
        currentCoordinatorId.set(announcement.getWinner().getWorkerId());
        System.out.println("[Worker " + workerId + "] recorded coordinator worker "
                + announcement.getWinner().getWorkerId() + " (JAC=" + announcement.getWinner().getJac()
                + ") for election " + announcement.getElectionId());
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
        System.out.println("[Worker " + workerId + "] MAX partial compute -> section="
                + numbers + ", partialMax=" + partialMax + ", JAC=" + updatedJac);
        return partialMax;
    }

    /**
     * Forwards a message to every neighbour except the hop it came from,
     * merging the participant sets of all echo replies. This is what lets an
     * election cover every reachable, active worker.
     */
    private ProcessedParticipants propagateToNeighbours(ElectionMessage message) {
        Set<CandidateInfo> participants = new LinkedHashSet<>(message.getParticipants());
        for (WorkerInfo neighbour : neighbours) {
            if (message.getSender() != null && message.getSender().getWorkerId() == neighbour.getWorkerId()) {
                continue;
            }
            WorkerService remote = lookupWorker(neighbour);
            if (remote == null) {
                continue;
            }
            try {
                ProcessedParticipants reply = remote.receiveElection(message);
                if (reply != null && reply.getParticipants() != null) {
                    participants.addAll(reply.getParticipants());
                }
            } catch (RemoteException e) {
                System.err.println("[Worker " + workerId + "] election propagation to " + neighbour
                        + " failed: " + e.getMessage());
            }
        }
        return new ProcessedParticipants(message.getElectionId(), message.getSender(), participants);
    }

    /**
     * Winner selection: lowest JAC wins; ties are broken by the highest worker id.
     */
    private CandidateInfo selectWinner(Collection<CandidateInfo> candidates) {
        return candidates.stream()
                .min(Comparator.comparingInt(CandidateInfo::getJac)
                        .thenComparing(Comparator.comparingInt(CandidateInfo::getWorkerId).reversed()))
                .orElseThrow(() -> new IllegalStateException("election produced no candidates"));
    }

    private void broadcastWinner(WinnerAnnouncement announcement) {
        for (CandidateInfo candidate : announcement.getParticipants()) {
            if (candidate.getWorkerId() == workerId) {
                continue;
            }
            WorkerService remote = lookupWorker(new WorkerInfo(candidate.getWorkerId(),
                    candidate.getHost(), candidate.getPort()));
            if (remote == null) {
                continue;
            }
            try {
                remote.announceWinner(announcement);
                System.out.println("[Worker " + workerId + "] winner announced to worker " + candidate.getWorkerId());
            } catch (RemoteException e) {
                System.err.println("[Worker " + workerId + "] could not announce winner to worker "
                        + candidate.getWorkerId() + ": " + e.getMessage());
            }
        }
    }

    /**
     * Rebuilds the neighbour ring from the Bootstrap Node's active-worker
     * registry. Each worker links to its immediate predecessor and successor in
     * the sorted registry, so the ring grows dynamically as workers register.
     */
    private synchronized int refreshNeighbours() {
        try {
            List<WorkerInfo> sortedAll = new ArrayList<>();
            boolean foundSelf = false;
            for (WorkerInfo info : bootstrapService.getActiveWorkers()) {
                if (info.getWorkerId() == workerId) {
                    foundSelf = true;
                }
                sortedAll.add(info);
            }
            if (!foundSelf) {
                sortedAll.add(new WorkerInfo(workerId, registeredHost, registeredPort));
            }
            sortedAll.sort(Comparator.comparingInt(WorkerInfo::getWorkerId));

            int n = sortedAll.size();
            if (n == 1) {
                neighbours.clear();
                return 0;
            }
            int idx = 0;
            for (int i = 0; i < n; i++) {
                if (sortedAll.get(i).getWorkerId() == workerId) {
                    idx = i;
                    break;
                }
            }
            WorkerInfo predecessor = sortedAll.get((idx - 1 + n) % n);
            WorkerInfo successor = sortedAll.get((idx + 1) % n);
            neighbours.clear();
            neighbours.add(predecessor);
            if (successor.getWorkerId() != predecessor.getWorkerId()) {
                neighbours.add(successor);
            }
            return neighbours.size();
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] neighbour sync failed: " + e.getMessage());
            return neighbours.size();
        }
    }

    private List<WorkerService> getReachableWorkerServices() throws RemoteException {
        if (bootstrapService == null) {
            return Collections.singletonList(this);
        }

        Map<Integer, WorkerInfo> activeWorkers = new HashMap<>();
        for (WorkerInfo worker : bootstrapService.getActiveWorkers()) {
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
                for (WorkerInfo neighbour : service.getNeighbours()) {
                    int neighbourId = neighbour.getWorkerId();
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

    private WorkerService lookupReachableWorker(Map<Integer, WorkerInfo> activeWorkers, int targetWorkerId) {
        if (targetWorkerId == workerId) {
            return this;
        }

        WorkerInfo worker = activeWorkers.get(targetWorkerId);
        if (worker == null) {
            return null;
        }

        return lookupWorker(worker);
    }

    private WorkerService lookupWorker(WorkerInfo target) {
        try {
            Registry registry = LocateRegistry.getRegistry(target.getHost(), target.getPort());
            return (WorkerService) registry.lookup(WorkerServer.SERVICE_NAME_PREFIX + target.getWorkerId());
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] cannot reach " + target + ": " + e.getMessage());
            return null;
        }
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