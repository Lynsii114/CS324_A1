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
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
 *   <li>The initiator broadcasts over its random peer neighbours. Every
 *       reachable, active worker adds its own {@link CandidateInfo} snapshot and re-sends
 *       the message to its neighbours (except the hop it came from), and echoes
 *       the merged participant set back. When all echoes return, every reachable
 *       worker has been discovered.</li>
 *   <li>The winner is the reachable worker with the lowest JAC; ties are broken
 *       by the highest worker id.</li>
 *   <li>The result is propagated through the worker network as a COORDINATOR
 *       {@link WinnerAnnouncement}. Each worker records it once and forwards it
 *       to its neighbours, so reachable workers agree on the same coordinator.</li>
 *   <li>The Bootstrap Node is never involved beyond looking up the active worker
 *       registry; it does not participate in the election.</li>
 * </ul>
 */
public class WorkerServiceImpl extends UnicastRemoteObject implements WorkerService {
    private static final long serialVersionUID = 1L;
    private static final int MAX_COORDINATOR_JOBS_PER_TERM = 5;
    private static final String DEFAULT_CLIENT_ID = "default";

    private final int workerId;
    private final String registeredHost;
    private final int registeredPort;
    private final BootstrapService bootstrapService;

    private final AtomicInteger jobAllocationCounter = new AtomicInteger(0);
    private final Map<String, AtomicInteger> coordinatorJobsByClient = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> coordinatorByClient = new ConcurrentHashMap<>();
    private final Set<String> processedElectionIds = ConcurrentHashMap.newKeySet();
    private final Set<String> processedCoordinatorIds = ConcurrentHashMap.newKeySet();
    private final Set<WorkerInfo> neighbours = ConcurrentHashMap.newKeySet();
    private final AtomicInteger localJobThreadCounter = new AtomicInteger(1);
    private final ExecutorService localJobExecutor;
    private final String leaderman = "cs324";

    public WorkerServiceImpl(int workerId, WorkerInfo self, BootstrapService bootstrapService)
            throws RemoteException {
        super();
        this.workerId = workerId;
        this.registeredHost = self.getHost();
        this.registeredPort = self.getPort();
        this.bootstrapService = bootstrapService;
        this.localJobExecutor = Executors.newFixedThreadPool(4, runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("worker-" + workerId + "-job-" + localJobThreadCounter.getAndIncrement());
            return thread;
        });
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
        return getCurrentCoordinatorId(DEFAULT_CLIENT_ID);
    }

    @Override
    public int getCurrentCoordinatorId(String clientId) throws RemoteException {
        return coordinatorFor(clientId).get();
    }

    @Override
    public void setCurrentCoordinatorId(int coordinatorId) throws RemoteException {
        setCurrentCoordinatorId(DEFAULT_CLIENT_ID, coordinatorId);
    }

    @Override
    public void setCurrentCoordinatorId(String clientId, int coordinatorId) throws RemoteException {
        String key = clientKey(clientId);
        coordinatorFor(key).set(coordinatorId);
        if (coordinatorId == NO_COORDINATOR || coordinatorId == workerId) {
            jobsFor(key).set(0);
        }
        System.out.println("[Worker " + workerId + "] coordinator for " + key + " set to " + coordinatorId);
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
        return initiateElection(DEFAULT_CLIENT_ID);
    }

    @Override
    public String initiateElection(String clientId) throws RemoteException {
        String key = clientKey(clientId);
        int existing = coordinatorFor(key).get();
        if (existing != NO_COORDINATOR) {
            return key + " coordinator already present: worker " + existing
                    + " - no election started (reset coordinators to " + NO_COORDINATOR + " first)";
        }

        refreshNeighbours();

        CandidateInfo self = new CandidateInfo(workerId, jobAllocationCounter.get(), registeredHost, registeredPort);
        String electionId = key + "-" + UUID.randomUUID();
        // The initiator marks its own election as processed so a message that
        // loops back to it is dropped instead of being handled twice.
        processedElectionIds.add(electionId);

        System.out.println("[Worker " + workerId + "] election " + electionId + " started for " + key
                + ", JAC=" + self.getJac() + ", neighbours=" + neighbours.size());

        ElectionMessage message = new ElectionMessage(electionId, self, self, new LinkedHashSet<>());
        ProcessedParticipants collected = propagateToNeighbours(message);

        Set<CandidateInfo> allParticipants = new LinkedHashSet<>(collected.getParticipants());
        allParticipants.add(self);

        CandidateInfo winner = selectWinner(allParticipants);
        System.out.println("[Worker " + workerId + "] election " + electionId + " winner: worker "
                + winner.getWorkerId() + " (JAC=" + winner.getJac() + ") across "
                + allParticipants.size() + " reachable workers");

        WinnerAnnouncement announcement = new WinnerAnnouncement(electionId, key, winner, allParticipants);
        announceWinner(announcement);

        return key + " coordinator elected: worker " + winner.getWorkerId() + " (JAC=" + winner.getJac()
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
        System.out.println("[Worker " + workerId + "] ELECTION received <- from worker "
                + message.getSender().getWorkerId() + ", election=" + message.getElectionId()
                + ", participants=" + participants);

        ElectionMessage forwarded = new ElectionMessage(message.getElectionId(), message.getOriginator(), self, participants);
        return propagateToNeighbours(forwarded);
    }

    @Override
    public void announceWinner(WinnerAnnouncement announcement) throws RemoteException {
        if (announcement == null || announcement.getWinner() == null) {
            return;
        }
        if (!processedCoordinatorIds.add(announcement.getElectionId())) {
            System.out.println("[Worker " + workerId + "] dropping duplicate COORDINATOR message "
                    + announcement.getElectionId());
            return;
        }

        String key = clientKey(announcement.getClientId());
        coordinatorFor(key).set(announcement.getWinner().getWorkerId());
        jobsFor(key).set(0);
        System.out.println("[Worker " + workerId + "] COORDINATOR received for " + key + " -> worker "
                + announcement.getWinner().getWorkerId() + " (JAC=" + announcement.getWinner().getJac()
                + ") for election " + announcement.getElectionId());

        refreshNeighbours();
        propagateCoordinatorToNeighbours(announcement);
    }

    @Override
    public int submitMaxJob(List<Integer> numbers) throws RemoteException {
        return submitMaxJob(DEFAULT_CLIENT_ID, numbers);
    }

    @Override
    public int submitMaxJob(String clientId, List<Integer> numbers) throws RemoteException {
        String key = clientKey(clientId);
        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be null or empty");
        }
        if (coordinatorFor(key).get() != workerId) {
            throw new RemoteException("Worker " + workerId
                    + " is not the coordinator for " + key + ". Current coordinator is " + coordinatorFor(key).get());
        }

        List<WorkerService> reachableWorkers = getReachableWorkerServices();
        int workerCount = Math.min(reachableWorkers.size(), numbers.size());
        if (workerCount == 0) {
            throw new RemoteException("No reachable workers are available for MAX job");
        }

        System.out.println("[Worker " + workerId + "] MAX job received for " + key + " -> numbers="
                + numbers + ", reachableWorkers=" + reachableWorkers.size()
                + ", assignedWorkers=" + workerCount);

        List<ChunkResult<Integer>> results = runParallelChunkJob("MAX", numbers, reachableWorkers, workerCount,
                WorkerService::computePartialMax);
        int finalMax = Integer.MIN_VALUE;
        for (ChunkResult<Integer> result : results) {
            finalMax = Math.max(finalMax, result.value());
            System.out.println("[Worker " + workerId + "] MAX partial result <- fromWorkerId="
                    + result.workerId() + ", partialMax=" + result.value()
                    + ", currentFinalMax=" + finalMax);
        }

        System.out.println("[Worker " + workerId + "] MAX final result -> max=" + finalMax);
        completeCoordinatorJob(key, "MAX");
        return finalMax;
    }

    @Override
    public int computePartialMax(List<Integer> numbers) throws RemoteException {
        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be null or empty");
        }

        return runLocalJob("MAX", numbers, () -> {
            int partialMax = Collections.max(numbers);
            int updatedJac = jobAllocationCounter.incrementAndGet();
            System.out.println("[Worker " + workerId + "] MAX partial compute -> section="
                    + numbers + ", partialMax=" + partialMax + ", JAC=" + updatedJac);
            return partialMax;
        });
    }

    @Override
    public long submitPrimeSumJob(List<Integer> numbers) throws RemoteException {
        return submitPrimeSumJob(DEFAULT_CLIENT_ID, numbers);
    }

    @Override
    public long submitPrimeSumJob(String clientId, List<Integer> numbers) throws RemoteException {
        String key = clientKey(clientId);
        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be null or empty");
        }
        if (coordinatorFor(key).get() != workerId) {
            throw new RemoteException("Worker " + workerId
                    + " is not the coordinator for " + key + ". Current coordinator is " + coordinatorFor(key).get());
        }

        List<WorkerService> reachableWorkers = getReachableWorkerServices();
        int workerCount = Math.min(reachableWorkers.size(), numbers.size());
        if (workerCount == 0) {
            throw new RemoteException("No reachable workers are available for PRIMESUM job");
        }

        System.out.println("[Worker " + workerId + "] PRIMESUM job received for " + key + " -> numbers="
                + numbers + ", reachableWorkers=" + reachableWorkers.size()
                + ", assignedWorkers=" + workerCount);

        List<ChunkResult<Long>> results = runParallelChunkJob("PRIMESUM", numbers, reachableWorkers, workerCount,
                WorkerService::computePartialPrimeSum);
        long finalSum = 0L;
        for (ChunkResult<Long> result : results) {
            finalSum += result.value();
            System.out.println("[Worker " + workerId + "] PRIMESUM partial result <- fromWorkerId="
                    + result.workerId() + ", partialSum=" + result.value()
                    + ", currentFinalSum=" + finalSum);
        }

        System.out.println("[Worker " + workerId + "] PRIMESUM final result -> sum=" + finalSum);
        completeCoordinatorJob(key, "PRIMESUM");
        return finalSum;
    }

    @Override
    public long computePartialPrimeSum(List<Integer> numbers) throws RemoteException {
        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be null or empty");
        }

        return runLocalJob("PRIMESUM", numbers, () -> {
            long partialSum = 0L;
            for (Integer number : numbers) {
                if (number != null && isPrime(number)) {
                    partialSum += number;
                }
            }
            int updatedJac = jobAllocationCounter.incrementAndGet();
            System.out.println("[Worker " + workerId + "] PRIMESUM partial compute -> section="
                    + numbers + ", partialSum=" + partialSum + ", JAC=" + updatedJac);
            return partialSum;
        });
    }

    @Override
    public int submitPrimeCountJob(List<Integer> numbers) throws RemoteException {
        return submitPrimeCountJob(DEFAULT_CLIENT_ID, numbers);
    }

    @Override
    public int submitPrimeCountJob(String clientId, List<Integer> numbers) throws RemoteException {
        String key = clientKey(clientId);
        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be null or empty");
        }
        if (coordinatorFor(key).get() != workerId) {
            throw new RemoteException("Worker " + workerId
                    + " is not the coordinator for " + key + ". Current coordinator is " + coordinatorFor(key).get());
        }

        List<WorkerService> reachableWorkers = getReachableWorkerServices();
        int workerCount = Math.min(reachableWorkers.size(), numbers.size());
        if (workerCount == 0) {
            throw new RemoteException("No reachable workers are available for PRIMECOUNT job");
        }

        System.out.println("[Worker " + workerId + "] PRIMECOUNT job received for " + key + " -> numbers="
                + numbers + ", reachableWorkers=" + reachableWorkers.size()
                + ", assignedWorkers=" + workerCount);

        List<ChunkResult<Integer>> results = runParallelChunkJob("PRIMECOUNT", numbers, reachableWorkers, workerCount,
                WorkerService::computePartialPrimeCount);
        int finalCount = 0;
        for (ChunkResult<Integer> result : results) {
            finalCount += result.value();
            System.out.println("[Worker " + workerId + "] PRIMECOUNT partial result <- fromWorkerId="
                    + result.workerId() + ", partialCount=" + result.value()
                    + ", currentFinalCount=" + finalCount);
        }

        System.out.println("[Worker " + workerId + "] PRIMECOUNT final result -> count=" + finalCount);
        completeCoordinatorJob(key, "PRIMECOUNT");
        return finalCount;
    }

    @Override
    public int computePartialPrimeCount(List<Integer> numbers) throws RemoteException {
        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be null or empty");
        }

        return runLocalJob("PRIMECOUNT", numbers, () -> {
            int partialCount = 0;
            for (Integer number : numbers) {
                if (number != null && isPrime(number)) {
                    partialCount++;
                }
            }
            int updatedJac = jobAllocationCounter.incrementAndGet();
            System.out.println("[Worker " + workerId + "] PRIMECOUNT partial compute -> section="
                    + numbers + ", partialCount=" + partialCount + ", JAC=" + updatedJac);
            return partialCount;
        });
    }

    private <T> T runLocalJob(String jobName, List<Integer> numbers, Callable<T> task) throws RemoteException {
        Future<T> future = localJobExecutor.submit(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("[Worker " + workerId + "] Starting " + jobName
                    + " on thread: " + threadName + ", section=" + numbers);
            try {
                T result = task.call();
                System.out.println("[Worker " + workerId + "] Completed " + jobName
                        + " on thread: " + threadName);
                return result;
            } catch (Exception e) {
                System.err.println("[Worker " + workerId + "] Failed " + jobName
                        + " on thread: " + threadName + " - " + e.getMessage());
                throw e;
            }
        });

        try {
            return future.get();
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new RemoteException(jobName + " local worker thread was interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new RemoteException(jobName + " local worker thread failed: " + cause.getMessage(), cause);
        }
    }

    public void shutdownLocalJobExecutor() {
        localJobExecutor.shutdown();
        try {
            if (!localJobExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                localJobExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            localJobExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private boolean isPrime(int value) {
        if (value < 2) {
            return false;
        }
        if (value == 2) {
            return true;
        }
        if (value % 2 == 0) {
            return false;
        }
        for (int divisor = 3; divisor <= value / divisor; divisor += 2) {
            if (value % divisor == 0) {
                return false;
            }
        }
        return true;
    }

    private synchronized void completeCoordinatorJob(String clientId, String jobName) throws RemoteException {
        String key = clientKey(clientId);
        AtomicInteger coordinatorJobsThisTerm = jobsFor(key);
        int completed = coordinatorJobsThisTerm.incrementAndGet();
        int updatedJac = jobAllocationCounter.incrementAndGet();
        System.out.println("[Worker " + workerId + "] TERM job completed for " + key + " -> jobType=" + jobName
                + ", jobsThisTerm=" + completed + "/" + MAX_COORDINATOR_JOBS_PER_TERM
                + ", coordinatorJAC=" + updatedJac);

        if (completed < MAX_COORDINATOR_JOBS_PER_TERM) {
            return;
        }

        System.out.println("[Worker " + workerId + "] TERM for " + key + " ended after "
                + MAX_COORDINATOR_JOBS_PER_TERM + " assigned jobs; starting automatic election");
        coordinatorJobsThisTerm.set(0);

        List<WorkerService> reachableWorkers = getReachableWorkerServices();
        for (WorkerService worker : reachableWorkers) {
            try {
                worker.setCurrentCoordinatorId(key, NO_COORDINATOR);
            } catch (RemoteException e) {
                System.err.println("[Worker " + workerId + "] TERM reset failed for a worker: "
                        + e.getMessage());
            }
        }

        String result = initiateElection(key);
        System.out.println("[Worker " + workerId + "] TERM automatic election result -> " + result);
    }

    private String clientKey(String clientId) {
        return clientId == null || clientId.isBlank() ? DEFAULT_CLIENT_ID : clientId.trim();
    }

    private AtomicInteger coordinatorFor(String clientId) {
        return coordinatorByClient.computeIfAbsent(clientKey(clientId), ignored -> new AtomicInteger(NO_COORDINATOR));
    }

    private AtomicInteger jobsFor(String clientId) {
        return coordinatorJobsByClient.computeIfAbsent(clientKey(clientId), ignored -> new AtomicInteger(0));
    }

    private <T> List<ChunkResult<T>> runParallelChunkJob(String jobName, List<Integer> numbers,
            List<WorkerService> reachableWorkers, int workerCount, ChunkTask<T> task) throws RemoteException {
        List<ChunkAssignment> assignments = createChunkAssignments(numbers, reachableWorkers, workerCount);
        AtomicInteger threadCounter = new AtomicInteger(1);
        ExecutorService executor = Executors.newFixedThreadPool(assignments.size(), runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("worker-" + workerId + "-" + jobName.toLowerCase()
                    + "-dispatch-" + threadCounter.getAndIncrement());
            return thread;
        });

        try {
            List<Future<ChunkResult<T>>> futures = new ArrayList<>();
            for (ChunkAssignment assignment : assignments) {
                Callable<ChunkResult<T>> callable = () -> {
                    System.out.println("[Worker " + workerId + "] " + jobName
                            + " assigning on " + Thread.currentThread().getName()
                            + " -> toWorkerId=" + assignment.workerId()
                            + ", section=" + assignment.chunk());
                    T value = task.compute(assignment.worker(), assignment.chunk());
                    return new ChunkResult<>(assignment.workerId(), assignment.chunk(), value);
                };
                futures.add(executor.submit(callable));
            }

            List<ChunkResult<T>> results = new ArrayList<>();
            for (Future<ChunkResult<T>> future : futures) {
                results.add(future.get());
            }
            return results;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException(jobName + " job interrupted while waiting for worker threads", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new RemoteException(jobName + " job failed in a worker thread: " + cause.getMessage(), cause);
        } finally {
            executor.shutdownNow();
        }
    }

    private List<ChunkAssignment> createChunkAssignments(List<Integer> numbers, List<WorkerService> workers,
            int workerCount) throws RemoteException {
        List<ChunkAssignment> assignments = new ArrayList<>();
        int start = 0;
        for (int index = 0; index < workerCount; index++) {
            int remainingNumbers = numbers.size() - start;
            int remainingWorkers = workerCount - index;
            int chunkSize = (remainingNumbers + remainingWorkers - 1) / remainingWorkers;
            List<Integer> chunk = new ArrayList<>(numbers.subList(start, start + chunkSize));
            WorkerService worker = workers.get(index);
            assignments.add(new ChunkAssignment(worker, worker.getWorkerId(), chunk));
            start += chunkSize;
        }
        return assignments;
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
                System.out.println("[Worker " + workerId + "] ELECTION forwarding -> worker "
                        + neighbour.getWorkerId() + ", election=" + message.getElectionId());
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

    private void propagateCoordinatorToNeighbours(WinnerAnnouncement announcement) {
        for (WorkerInfo neighbour : neighbours) {
            WorkerService remote = lookupWorker(neighbour);
            if (remote == null) {
                continue;
            }
            try {
                remote.announceWinner(announcement);
                System.out.println("[Worker " + workerId + "] COORDINATOR forwarding -> worker "
                        + neighbour.getWorkerId());
            } catch (RemoteException e) {
                System.err.println("[Worker " + workerId + "] COORDINATOR forwarding to worker "
                        + neighbour.getWorkerId() + " failed: " + e.getMessage());
            }
        }
    }

    /**
     * Rebuilds a random connected neighbour set from the Bootstrap Node's
     * active-worker registry. Workers are placed into a shuffled ring and each
     * worker links to the adjacent peers in that random ring, so elections can
     * still reach the full active cluster.
     */
    private synchronized int refreshNeighbours() {
        try {
            List<WorkerInfo> randomRing = new ArrayList<>();
            for (WorkerInfo info : bootstrapService.getActiveWorkers()) {
                randomRing.add(info);
            }
            boolean foundSelf = randomRing.stream().anyMatch(info -> info.getWorkerId() == workerId);
            if (!foundSelf) {
                randomRing.add(new WorkerInfo(workerId, registeredHost, registeredPort));
            }
            randomRing.sort(Comparator.comparingInt(WorkerInfo::getWorkerId));
            Collections.shuffle(randomRing, new Random(randomTopologySeed(randomRing)));

            neighbours.clear();

            if (randomRing.size() > 1) {
                int index = indexOfSelf(randomRing);
                WorkerInfo previous = randomRing.get((index - 1 + randomRing.size()) % randomRing.size());
                WorkerInfo next = randomRing.get((index + 1) % randomRing.size());
                neighbours.add(previous);
                if (next.getWorkerId() != previous.getWorkerId()) {
                    neighbours.add(next);
                }
            }

            System.out.println("[Worker " + workerId + "] RANDOM neighbours -> " + neighbours);
            return neighbours.size();
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] neighbour sync failed: " + e.getMessage());
            return neighbours.size();
        }
    }

    private long randomTopologySeed(List<WorkerInfo> activeWorkers) {
        long seed = WorkerClusterConfig.RANDOM_TOPOLOGY_SEED;
        for (WorkerInfo info : activeWorkers) {
            seed = 31 * seed + info.getWorkerId();
        }
        return seed;
    }

    private int indexOfSelf(List<WorkerInfo> workers) {
        for (int i = 0; i < workers.size(); i++) {
            if (workers.get(i).getWorkerId() == workerId) {
                return i;
            }
        }
        return 0;
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

    @FunctionalInterface
    private interface ChunkTask<T> {
        T compute(WorkerService worker, List<Integer> chunk) throws Exception;
    }

    private record ChunkAssignment(WorkerService worker, int workerId, List<Integer> chunk) {
    }

    private record ChunkResult<T>(int workerId, List<Integer> chunk, T value) {
    }

    @Override
    public String toString() {
        return "WorkerServiceImpl{"
                + "workerId=" + workerId
                + ", jobAllocationCounter=" + jobAllocationCounter.get()
                + ", neighbours=" + neighbours
                + ", coordinators=" + coordinatorByClient
                + ", leaderman='" + leaderman + '\''
                + '}';
    }
}
