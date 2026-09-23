package com.cs324.worker;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Basic Worker Node state holder. It does not run leader election or execute jobs.
 */
public class WorkerServiceImpl extends UnicastRemoteObject implements WorkerService {
    private static final long serialVersionUID = 1L;

    private final int workerId;
    private final AtomicInteger jobAllocationCounter = new AtomicInteger(0);
    private final Set<Integer> neighbours = ConcurrentHashMap.newKeySet();
    private final AtomicInteger currentCoordinatorId;
    private final String leaderman = "cs324";

    public WorkerServiceImpl(int workerId) throws RemoteException {
        super();
        this.workerId = workerId;
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
    public String getLeaderman() throws RemoteException {
        return leaderman;
    }

    @Override
    public int countPrimes(List<Integer> numbers) throws RemoteException {
        if (numbers == null) {
            throw new IllegalArgumentException("numbers must not be null");
        }
        int count = 0;
        for (Integer value : numbers) {
            if (value == null) {
                throw new IllegalArgumentException("numbers must not contain null values");
            }
            if (isPrime(value)) count++;
        }
        return count;
    }

    private static boolean isPrime(int number) {
        if (number < 2) return false;
        if (number == 2) return true;
        if (number % 2 == 0) return false;
        for (int divisor = 3; divisor <= number / divisor; divisor += 2) {
            if (number % divisor == 0) return false;
        }
        return true;
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
