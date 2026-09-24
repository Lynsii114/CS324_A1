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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 *
 * <p><b>Concurrency strategy (5-job coordinator terms + multithreaded
 * execution):</b> jobs arrive over RMI and can overlap, so all shared mutable
 * state is guarded by concurrent primitives instead of coarse locks:
 * <ul>
 *   <li>{@link #jobAllocationCounter} (JAC) - {@link AtomicInteger}.</li>
 *   <li>{@link #jobsThisTerm} (per-term submitted-job counter, isolated from the
 *       JAC) - {@link AtomicInteger}.</li>
 *   <li>{@link #currentCoordinatorId} - {@link AtomicInteger}.</li>
 *   <li>{@link #processedElectionIds} - a concurrent key set, so an election
 *       message is processed at most once even under concurrent arrivals.</li>
 *   <li>{@link #neighbours} - a concurrent key set; the whole ring is swapped
 *       atomically inside the {@code synchronized} {@link #refreshNeighbours()}.</li>
 * </ul>
 * A single {@link ExecutorService} ({@link #jobExecutor}) runs submitted jobs
 * and partial computations concurrently. The pool is sized larger than the
 * coordinator term limit so the coordinator can always borrow another thread for
 * its own local section without deadlocking.
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
    private static final String leaderman = "cs324";

    /**
     * Number of submitted jobs handled as coordinator during the current term.
     * Deliberately separate from the JAC: the JAC counts every single job
     * section a worker processes; this counter counts only complete client jobs
     * submitted to this worker while it is the term coordinator.
     */
    private final AtomicInteger jobsThisTerm = new AtomicInteger(0);

    private final AtomicInteger jobThreadCounter = new AtomicInteger(0);

    /**
     * Guards against a worker running two elections at once (e.g. its background
     * auto-election check colliding with the re-election that ends a term, or
     * with an election message flood it is currently participating in).
     */
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);

    /**
     * Runs submitted jobs and partial computations concurrently. Daemon threads
     * never keep the JVM alive; RMI exports already do that. Built in the
     * constructor because the pool must be sized with the worker id in hand.
     */
    private final ExecutorService jobExecutor;

    /**
     * Periodically checks whether the cluster is missing a coordinator and, if
     * so, quietly starts a leader election. This is what lets a freshly started
     * cluster elect a coordinator automatically (no manual trigger needed) and
     * lets the cluster re-elect itself after a coordinator reset.
     * Built in the constructor because the thread name needs the worker id.
     */
    private final ScheduledExecutorService electionScheduler;

    public WorkerServiceImpl(int workerId, WorkerInfo self, BootstrapService bootstrapService)
            throws RemoteException {
        super();
        this.workerId = workerId;
        this.registeredHost = self.getHost();
        this.registeredPort = self.getPort();
        this.bootstrapService = bootstrapService;
        this.electionScheduler = Executors.newSingleThreadScheduledExecutor(
                runnable -> {
                    Thread thread = new Thread(runnable, "election-check-" + workerId);
                    thread.setDaemon(true);
                    return thread;
                });
        this.jobExecutor = Executors.newFixedThreadPool(
                Math.max(WorkerService.COORDINATOR_TERM_LIMIT + 1, Runtime.getRuntime().availableProcessors()),
                runnable -> {
                    Thread thread = new Thread(runnable,
                            "job-" + workerId + "-" + jobThreadCounter.incrementAndGet());
                    thread.setDaemon(true);
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
    public int recordJobAllocation() throws RemoteException {
        int updated = jobAllocationCounter.incrementAndGet();
        System.out.println("[Worker " + workerId + "] JAC changed to " + updated
                + " (assigned a job section to another worker)");
        return updated;
    }

    @Override
    public int getJobsThisTerm() throws RemoteException {
        return jobsThisTerm.get();
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
        recordCoordinator(coordinatorId);
    }

    /**
     * Atomically records the current coordinator. Whenever this node becomes the
     * coordinator itself, a fresh term begins: the per-term submitted-job counter
     * is reset to zero so the new coordinator gets a clean five-job budget.
     */
    private void recordCoordinator(int coordinatorId) {
        int previous = currentCoordinatorId.getAndSet(coordinatorId);
        if (coordinatorId == workerId) {
            jobsThisTerm.set(0);
        }
        System.out.println("[Worker " + workerId + "] coordinator set to " + coordinatorId
                + (previous == coordinatorId ? "" : " (was " + previous + ")"));
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
        if (!electionInProgress.compareAndSet(false, true)) {
            return "Election already in progress - ignoring new election request";
        }
        try {
            int existing = currentCoordinatorId.get();
            if (existing != NO_COORDINATOR) {
                return "Coordinator already present: worker " + existing
                        + " - no election started (an election runs only when no coordinator "
                        + "is active, e.g. after a 5-job term ends)";
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
            recordCoordinator(winner.getWorkerId());

            System.out.println("[Worker " + workerId + "] election " + electionId + " winner: worker "
                    + winner.getWorkerId() + " (JAC=" + winner.getJac() + ") across "
                    + allParticipants.size() + " reachable workers");

            WinnerAnnouncement announcement = new WinnerAnnouncement(electionId, winner, allParticipants);
            broadcastWinner(announcement);

            return "Coordinator elected: worker " + winner.getWorkerId() + " (JAC=" + winner.getJac()
                    + ") across " + allParticipants.size() + " reachable workers [electionId=" + electionId + "]";
        } finally {
            electionInProgress.set(false);
        }
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

        boolean wasInProgress = electionInProgress.getAndSet(true);
        try {
            refreshNeighbours();

            CandidateInfo self = new CandidateInfo(workerId, jobAllocationCounter.get(), registeredHost, registeredPort);
            Set<CandidateInfo> participants = new LinkedHashSet<>(message.getParticipants());
            participants.add(self);

            ElectionMessage forwarded = new ElectionMessage(message.getElectionId(), message.getOriginator(), self, participants);
            return propagateToNeighbours(forwarded);
        } finally {
            electionInProgress.set(wasInProgress);
        }
    }

    /**
     * Starts the background auto-election checks. Called once the worker has
     * registered with the Bootstrap Node and has its neighbours, so each election
     * covers the full cluster. The staggered initial delay keeps the workers from
     * all starting elections in the same instant.
     */
    public void startAutoElectionChecks() {
        long initialDelay = Math.max(FIRST_WORKER_DELAY_MS, (long) workerId * WORKER_DELAY_STEP_MS);
        electionScheduler.scheduleWithFixedDelay(this::autoElectionTick,
                initialDelay, AUTO_ELECTION_PERIOD_MS, TimeUnit.MILLISECONDS);
        System.out.println("[Worker " + workerId + "] auto-election checks started (first check in "
                + initialDelay + " ms)");
    }

    private static final long FIRST_WORKER_DELAY_MS = 2_500;
    private static final long WORKER_DELAY_STEP_MS = 2_000;
    private static final long AUTO_ELECTION_PERIOD_MS = 4_000;

    /**
     * Fires on the scheduler thread. If the cluster currently has no coordinator
     * and this worker is not taking part in another election, it quietly starts
     * one. Once a coordinator exists the check does nothing, so a healthy cluster
     * never re-elects.
     */
    private void autoElectionTick() {
        if (currentCoordinatorId.get() != NO_COORDINATOR || electionInProgress.get()) {
            return;
        }
        try {
            String result = initiateElection();
            if (!result.startsWith("Coordinator already present")) {
                System.out.println("[Worker " + workerId + "] auto-election result: " + result);
            }
        } catch (RemoteException e) {
            System.err.println("[Worker " + workerId + "] auto-election failed: " + e.getMessage());
        }
    }

    @Override
    public void announceWinner(WinnerAnnouncement announcement) throws RemoteException {
        if (announcement == null || announcement.getWinner() == null) {
            return;
        }
        processedElectionIds.add(announcement.getElectionId());
        recordCoordinator(announcement.getWinner().getWorkerId());
        System.out.println("[Worker " + workerId + "] recorded coordinator worker "
                + announcement.getWinner().getWorkerId() + " (JAC=" + announcement.getWinner().getJac()
                + ") for election " + announcement.getElectionId());
    }

    @Override
    public int submitMaxJob(List<Integer> numbers) throws RemoteException {
        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be null or empty");
        }
        requireCoordinator();

        int termSlot = claimTermSlot();
        return runJob(termSlot, () -> executeSubmitMaxJob(numbers));
    }

    /**
     * Runs one submitted coordinator job on the executor. The term budget is
     * consumed at submission time, and after the last accepted job of a term
     * completes the coordinator's term ends and a fresh election is triggered
     * (see {@link #maybeEndCoordinatorTerm()}).
     */
    private <T> T runJob(int termSlot, Callable<T> job) throws RemoteException {
        System.out.println("[Worker " + workerId + "] submitted job #" + termSlot
                + " -> queued on executor");
        try {
            return jobExecutor.submit(() -> {
                System.out.println("[Worker " + workerId + "] executing submitted job #" + termSlot
                        + " on thread " + Thread.currentThread().getName());
                return job.call();
            }).get();
        } catch (ExecutionException e) {
            throw unwrapExecution(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Job interrupted", e);
        } finally {
            maybeEndCoordinatorTerm();
        }
    }

    private void requireCoordinator() throws RemoteException {
        if (currentCoordinatorId.get() != workerId) {
            throw new RemoteException("Worker " + workerId
                    + " is not the coordinator. Current coordinator is " + currentCoordinatorId.get());
        }
    }

    /**
     * Reserves one of the coordinator's per-term submitted-job slots. An atomic
     * check-then-increment keeps the counter at most {@link
     * WorkerService#COORDINATOR_TERM_LIMIT} for the current term.
     */
    private int claimTermSlot() throws RemoteException {
        for (; ; ) {
            int current = jobsThisTerm.get();
            if (current >= WorkerService.COORDINATOR_TERM_LIMIT) {
                throw new RemoteException("Coordinator term ended after "
                        + WorkerService.COORDINATOR_TERM_LIMIT + " jobs; no jobs can be processed "
                        + "until the next leader election completes");
            }
            if (jobsThisTerm.compareAndSet(current, current + 1)) {
                return current + 1;
            }
        }
    }

    /**
     * Ends the coordinator's term as soon as the last accepted job completes:
     * the coordinator demotes itself and immediately starts a new distributed
     * leader election. The compare-and-set ensures exactly one thread performs
     * the term hand-over even when several jobs complete at once.
     */
    private void maybeEndCoordinatorTerm() {
        if (jobsThisTerm.get() < WorkerService.COORDINATOR_TERM_LIMIT) {
            return;
        }
        if (!currentCoordinatorId.compareAndSet(workerId, NO_COORDINATOR)) {
            return; // another finished job already ended this term
        }
        System.out.println("[Worker " + workerId + "] completed "
                + WorkerService.COORDINATOR_TERM_LIMIT + " jobs this term - ending term "
                + "and starting a new leader election");
        try {
            String result = initiateElection();
            System.out.println("[Worker " + workerId + "] term re-election result: " + result);
        } catch (RemoteException e) {
            System.err.println("[Worker " + workerId + "] term re-election failed: " + e.getMessage());
        }
    }

    private RemoteException unwrapExecution(ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RemoteException remoteException) {
            return remoteException;
        }
        return new RemoteException("Job failed: " + cause, cause);
    }

    private int executeSubmitMaxJob(List<Integer> numbers) throws RemoteException {
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

            if (assignedWorkerId != workerId) {
                recordJobAllocation();
            }

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
        return runPartialJob(() -> {
            int partialMax = Collections.max(numbers);
            System.out.println("[Worker " + workerId + "] MAX partial compute -> section="
                    + numbers + ", partialMax=" + partialMax);
            return partialMax;
        });
    }

    /** Runs a worker-side partial computation on the shared job executor. */
    private <T> T runPartialJob(Callable<T> job) throws RemoteException {
        try {
            return jobExecutor.submit(job).get();
        } catch (ExecutionException e) {
            throw unwrapExecution(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Job interrupted", e);
        }
    }

    @Override
    public int submitPrimeCount(List<Integer> numbers) throws RemoteException {
        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be null or empty");
        }
        requireCoordinator();

        int termSlot = claimTermSlot();
        return runJob(termSlot, () -> executeSubmitPrimeCount(numbers));
    }

    private int executeSubmitPrimeCount(List<Integer> numbers) throws RemoteException {
        List<WorkerService> reachableWorkers = getReachableWorkerServices();
        int workerCount = Math.min(reachableWorkers.size(), numbers.size());
        if (workerCount == 0) {
            throw new RemoteException("No reachable workers are available for PRIMECOUNT job");
        }

        System.out.println("[Worker " + workerId + "] PRIMECOUNT job received -> numbers="
                + numbers + ", reachableWorkers=" + reachableWorkers.size()
                + ", assignedWorkers=" + workerCount);

        int totalPrimes = 0;
        int start = 0;
        for (int index = 0; index < workerCount; index++) {
            int remainingNumbers = numbers.size() - start;
            int remainingWorkers = workerCount - index;
            int chunkSize = (remainingNumbers + remainingWorkers - 1) / remainingWorkers;
            List<Integer> chunk = new ArrayList<>(numbers.subList(start, start + chunkSize));
            WorkerService worker = reachableWorkers.get(index);
            int assignedWorkerId = worker.getWorkerId();

            if (assignedWorkerId != workerId) {
                recordJobAllocation();
            }

            try {
                System.out.println("[Worker " + workerId + "] PRIMECOUNT assigning -> toWorkerId="
                        + assignedWorkerId + ", section=" + chunk);
                int partialCount = worker.countPrimes(chunk);
                totalPrimes += partialCount;
                System.out.println("[Worker " + workerId + "] PRIMECOUNT partial result <- fromWorkerId="
                        + assignedWorkerId + ", primes=" + partialCount
                        + ", runningTotal=" + totalPrimes);
            } catch (Exception e) {
                throw new RemoteException("PRIMECOUNT job failed while assigning worker "
                        + assignedWorkerId, e);
            }

            start += chunkSize;
        }

        System.out.println("[Worker " + workerId + "] PRIMECOUNT final result -> primes=" + totalPrimes);
        return totalPrimes;
    }

    @Override
    public int countPrimes(List<Integer> numbers) throws RemoteException {
        if (numbers == null) {
            throw new IllegalArgumentException("numbers must not be null");
        }
        return runPartialJob(() -> {
            if (numbers.contains(null)) {
                throw new IllegalArgumentException("numbers must not contain null values");
            }
            int count = 0;
            for (Integer value : numbers) {
                if (isPrime(value)) {
                    count++;
                }
            }
            System.out.println("[Worker " + workerId + "] PRIMECOUNT partial compute -> section="
                    + numbers + ", primes=" + count);
            return count;
        });
    }

    @Override
    public long submitPrimeSum(int start, int end) throws RemoteException {
        if (start < 1 || end < start) {
            throw new IllegalArgumentException("invalid range: start=" + start + ", end=" + end);
        }
        requireCoordinator();

        int termSlot = claimTermSlot();
        return runJob(termSlot, () -> executeSubmitPrimeSum(start, end));
    }

    private long executeSubmitPrimeSum(int start, int end) throws RemoteException {
        List<WorkerService> reachableWorkers = getReachableWorkerServices();
        int workerCount = Math.min(reachableWorkers.size(), end - start + 1);
        if (workerCount == 0) {
            throw new RemoteException("No reachable workers are available for PRIMESUM job");
        }

        System.out.println("[Worker " + workerId + "] PRIMESUM job received -> start=" + start
                + ", end=" + end + ", reachableWorkers=" + reachableWorkers.size()
                + ", assignedWorkers=" + workerCount);

        long totalSum = 0;
        int segmentStart = start;
        for (int index = 0; index < workerCount; index++) {
            int remainingLength = end - segmentStart + 1;
            int remainingWorkers = workerCount - index;
            int chunkSize = (remainingLength + remainingWorkers - 1) / remainingWorkers;
            int segmentEnd = segmentStart + chunkSize - 1;

            WorkerService worker = reachableWorkers.get(index);
            int assignedWorkerId = worker.getWorkerId();

            if (assignedWorkerId != workerId) {
                recordJobAllocation();
            }

            try {
                System.out.println("[Worker " + workerId + "] PRIMESUM assigning -> toWorkerId="
                        + assignedWorkerId + ", range=[" + segmentStart + ", " + segmentEnd + "]");
                long partialSum = worker.sumPrimeRange(segmentStart, segmentEnd);
                totalSum += partialSum;
                System.out.println("[Worker " + workerId + "] PRIMESUM partial result <- fromWorkerId="
                        + assignedWorkerId + ", sum=" + partialSum + ", runningTotal=" + totalSum);
            } catch (Exception e) {
                throw new RemoteException("PRIMESUM job failed while assigning worker "
                        + assignedWorkerId, e);
            }

            segmentStart = segmentEnd + 1;
        }

        System.out.println("[Worker " + workerId + "] PRIMESUM final result -> sum=" + totalSum);
        return totalSum;
    }

    @Override
    public long sumPrimeRange(int start, int end) throws RemoteException {
        if (start < 1 || end < start) {
            throw new IllegalArgumentException("invalid range: start=" + start + ", end=" + end);
        }
        return runPartialJob(() -> {
            long sum = 0;
            for (int n = start; n <= end; n++) {
                if (isPrime(n)) {
                    sum += n;
                }
            }
            System.out.println("[Worker " + workerId + "] PRIMESUM partial compute -> range=["
                    + start + ", " + end + "], sum=" + sum);
            return sum;
        });
    }

    private static boolean isPrime(int number) {
        if (number < 2) {
            return false;
        }
        if (number == 2) {
            return true;
        }
        if (number % 2 == 0) {
            return false;
        }
        for (int divisor = 3; divisor <= number / divisor; divisor += 2) {
            if (number % divisor == 0) {
                return false;
            }
        }
        return true;
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

    private synchronized int refreshNeighbours() {
        try {
            List<WorkerInfo> active = new ArrayList<>(bootstrapService.getActiveWorkers());
            boolean foundSelf = active.stream().anyMatch(i -> i.getWorkerId() == workerId);
            if (!foundSelf) {
                active.add(new WorkerInfo(workerId, registeredHost, registeredPort));
            }
            if (active.size() <= 1) {
                neighbours.clear();
                return 0;
            }

            active.sort(Comparator.comparingInt(WorkerInfo::getWorkerId));
            List<WorkerInfo> others = new ArrayList<>(active);
            others.removeIf(i -> i.getWorkerId() == workerId);

            neighbours.clear();

            // Ring backbone: predecessor + successor guarantee reachability so the
            // cluster can always reach agreement on a single coordinator.
            int idx = 0;
            for (int i = 0; i < active.size(); i++) {
                if (active.get(i).getWorkerId() == workerId) {
                    idx = i;
                    break;
                }
            }
            neighbours.add(active.get((idx - 1 + active.size()) % active.size()));
            WorkerInfo successor = active.get((idx + 1) % active.size());
            if (successor.getWorkerId() != workerId) {
                neighbours.add(successor);
            }

            // Unstructured extra links: a new worker is randomly connected to an
            // active worker whenever it (re)joins, so the network stays random and
            // each worker holds only a subset of the other workers.
            WorkerInfo randomPeer = bootstrapService.getRandomWorker();
            if (randomPeer != null && randomPeer.getWorkerId() != workerId
                    && neighbours.add(randomPeer)) {
                System.out.println("[Worker " + workerId + "] random link added -> "
                        + randomPeer.getWorkerId() + " (neighbours=" + neighbours.size() + ")");
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
        Map<Integer, Integer> jacByWorker = new HashMap<>();
        for (Integer reachableWorkerId : reachableWorkerIds) {
            WorkerService service = lookupReachableWorker(activeWorkers, reachableWorkerId);
            if (service != null) {
                services.add(service);
                jacByWorker.put(reachableWorkerId, readJAC(service));
            }
        }
        // Distribute work to the least-loaded workers first: ascending JAC,
        // then ascending worker id as a stable tie-break.
        services.sort(Comparator
                .comparingInt((WorkerService s) -> jacByWorker.getOrDefault(readWorkerId(s), Integer.MAX_VALUE))
                .thenComparingInt(s -> readWorkerId(s)));
        return services;
    }

    private int readJAC(WorkerService service) {
        try {
            return service.getJobAllocationCounter();
        } catch (Exception e) {
            return Integer.MAX_VALUE;
        }
    }

    private int readWorkerId(WorkerService service) {
        try {
            return service.getWorkerId();
        } catch (Exception e) {
            return Integer.MAX_VALUE;
        }
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