import com.cs324.frontend.client.DistriLabClientGui;

/**
 * Simple terminal launcher.
 *
 * <p>Usage: java Client <clientId>
 */
public class Client {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java Client <clientId>");
            System.err.println("Example: java Client 1");
            System.exit(1);
        }

        int clientId = parseClientId(args[0]);
        if (clientId != 1 && clientId != 2) {
            throw new IllegalArgumentException("clientId must be 1 or 2");
        }

        DistriLabClientGui.main(new String[] {"Client " + clientId});
    }

    private static int parseClientId(String rawClientId) {
        try {
            return Integer.parseInt(rawClientId.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("clientId must be 1 or 2", e);
        }
    }
}
