package com.cs324.backend.api;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * RMI-exposed contract for a Worker Node. Exposes worker state, neighbour
 * management and the distributed leader election protocol.
 */
public interface WorkerService extends Remote {

    /** Sentinel used when no coordinator has been elected yet. */
    int NO_COORDINATOR = -1;

    int getWorkerId() throws RemoteException;

    int getJobAllocationCounter() throws RemoteException;

    /** Increases the Job Acceptance Counter (JAC) of this worker by one. */
    int incrementJobAllocationCounter() throws RemoteException;

    int resetJobAllocationCounter() throws RemoteException;

    List<WorkerInfo> getNeighbours() throws RemoteException;

    void addNeighbour(WorkerInfo worker) throws RemoteException;

    /** Adds a neighbour by id, resolving its host/port from the Bootstrap Node. */
    void addNeighbour(int workerId) throws RemoteException;

    void removeNeighbour(int workerId) throws RemoteException;

    /** Current coordinator id, or {@link #NO_COORDINATOR} if none is present. */
    int getCurrentCoordinatorId() throws RemoteException;

    int getCurrentCoordinatorId(String clientId) throws RemoteException;

    void setCurrentCoordinatorId(int coordinatorId) throws RemoteException;

    void setCurrentCoordinatorId(String clientId, int coordinatorId) throws RemoteException;

    String getLeaderman() throws RemoteException;

    /**
     * Rebuilds the random connected neighbour set from the Bootstrap Node's
     * registry of active workers. Returns the new number of neighbours.
     */
    int syncNeighbours() throws RemoteException;

    /**
     * Starts a distributed leader election. Only proceeds if no coordinator is
     * present; otherwise returns the current coordinator.
     */
    String initiateElection() throws RemoteException;

    String initiateElection(String clientId) throws RemoteException;

    /**
     * Handles an incoming ELECTION message. Returns {@code null} if the election
     * was already processed (deduplication), otherwise the merged participant set.
     */
    ProcessedParticipants receiveElection(ElectionMessage message) throws RemoteException;

    /** Records the elected coordinator for a finished election. */
    void announceWinner(WinnerAnnouncement announcement) throws RemoteException;

    int submitMaxJob(List<Integer> numbers) throws RemoteException;

    int submitMaxJob(String clientId, List<Integer> numbers) throws RemoteException;

    int computePartialMax(List<Integer> numbers) throws RemoteException;

    long submitPrimeSumJob(List<Integer> numbers) throws RemoteException;

    long submitPrimeSumJob(String clientId, List<Integer> numbers) throws RemoteException;

    long computePartialPrimeSum(List<Integer> numbers) throws RemoteException;

    int submitPrimeCountJob(List<Integer> numbers) throws RemoteException;

    int submitPrimeCountJob(String clientId, List<Integer> numbers) throws RemoteException;

    int computePartialPrimeCount(List<Integer> numbers) throws RemoteException;
}
