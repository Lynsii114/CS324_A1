import com.cs324.backend.worker.WorkerClusterConfig;
import com.cs324.backend.worker.WorkerServer;

/**
 * Simple terminal launcher.
 *
 * <p>Usage: java Worker <workerId>
 */
public class Worker {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java Worker <workerId>");
            System.err.println("Example: java Worker 1");
            System.exit(1);
        }

        int workerId = parseWorkerId(args[0]);
        int workerPort = WorkerClusterConfig.portFor(workerId);
        WorkerServer.main(new String[] {
                String.valueOf(workerId),
                String.valueOf(workerPort),
                WorkerClusterConfig.DEFAULT_HOST,
                "1099",
                WorkerClusterConfig.DEFAULT_HOST
        });
    }

    private static int parseWorkerId(String rawWorkerId) {
        try {
            return Integer.parseInt(rawWorkerId.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("workerId must be a number from 1 to 4", e);
        }
    }
}
