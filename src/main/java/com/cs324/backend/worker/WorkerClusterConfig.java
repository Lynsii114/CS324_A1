package com.cs324.backend.worker;

/**
 * Central configuration for the six-worker cluster.
 * Worker IDs are 1..6 and each worker owns its own RMI registry port,
 * so every worker runs in an isolated environment.
 */
public final class WorkerClusterConfig {

    public static final int WORKER_COUNT = 6;
    public static final int FIRST_WORKER_ID = 1;
    public static final int LAST_WORKER_ID = FIRST_WORKER_ID + WORKER_COUNT - 1;
    public static final int BASE_WORKER_PORT = 5001;
    public static final String DEFAULT_HOST = "localhost";
    public static final String LOG_DIR = "logs";

    private WorkerClusterConfig() {
    }

    /** Returns the configured worker IDs: 1, 2, 3, 4, 5, 6. */
    public static int[] workerIds() {
        int[] ids = new int[WORKER_COUNT];
        for (int i = 0; i < WORKER_COUNT; i++) {
            ids[i] = FIRST_WORKER_ID + i;
        }
        return ids;
    }

    /** Maps a worker ID to its dedicated RMI registry port (1 -> 5001 ... 6 -> 5006). */
    public static int portFor(int workerId) {
        if (workerId < FIRST_WORKER_ID || workerId > LAST_WORKER_ID) {
            throw new IllegalArgumentException(
                    "workerId must be between " + FIRST_WORKER_ID + " and " + LAST_WORKER_ID + ", got " + workerId);
        }
        return BASE_WORKER_PORT + (workerId - FIRST_WORKER_ID);
    }

    /** RMI binding name of a worker inside its own registry. */
    public static String serviceNameFor(int workerId) {
        return WorkerServer.SERVICE_NAME_PREFIX + workerId;
    }
}
