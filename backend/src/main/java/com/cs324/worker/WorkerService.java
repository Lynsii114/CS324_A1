package com.cs324.worker;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * RMI-exposed contract for a Worker Node. This exposes worker state only;
 * leader election and job processing are intentionally not implemented yet.
 */
public interface WorkerService extends Remote {

    int getWorkerId() throws RemoteException;

    int getJobAllocationCounter() throws RemoteException;

    List<Integer> getNeighbours() throws RemoteException;

    void addNeighbour(int workerId) throws RemoteException;

    void removeNeighbour(int workerId) throws RemoteException;

    int getCurrentCoordinatorId() throws RemoteException;

    void setCurrentCoordinatorId(int coordinatorId) throws RemoteException;

    void startElection() throws RemoteException;

    void receiveElectionMessage(ElectionMessage message) throws RemoteException;

    void broadcastCoordinator(int coordinatorId) throws RemoteException;

    void receiveCoordinatorMessage(CoordinatorMessage message) throws RemoteException;

    String getLeaderman() throws RemoteException;
}
