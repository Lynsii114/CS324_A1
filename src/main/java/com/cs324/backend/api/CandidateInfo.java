package com.cs324.backend.api;

import java.io.Serializable;
import java.util.Objects;

/**
 * Snapshot of a worker (workerId, JAC, host, port) taken at the moment an
 * election runs. Sent over RMI inside {@link ElectionMessage}, so it must be
 * Serializable.
 */
public final class CandidateInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int workerId;
    private final int jac;
    private final String host;
    private final int port;

    public CandidateInfo(int workerId, int jac, String host, int port) {
        this.workerId = workerId;
        this.jac = jac;
        this.host = host;
        this.port = port;
    }

    public int getWorkerId() {
        return workerId;
    }

    public int getJac() {
        return jac;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CandidateInfo)) return false;
        CandidateInfo other = (CandidateInfo) o;
        return workerId == other.workerId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(workerId);
    }

    @Override
    public String toString() {
        return "CandidateInfo{workerId=" + workerId + ", jac=" + jac
                + ", host='" + host + "', port=" + port + "}";
    }
}