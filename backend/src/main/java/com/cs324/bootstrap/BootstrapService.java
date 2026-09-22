package com.cs324.bootstrap;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * RMI-exposed contract for the Bootstrap Node. It only tracks membership -
 * it never takes part in leader election or job processing.
 */
public interface BootstrapService extends Remote {

    void registerWorker(WorkerInfo worker) throws RemoteException;

    void unregisterWorker(String workerId) throws RemoteException;

    List<WorkerInfo> getActiveWorkers() throws RemoteException;

    WorkerInfo getRandomWorker() throws RemoteException;
}
