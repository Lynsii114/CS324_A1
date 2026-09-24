package com.cs324.frontend.client;

import com.cs324.backend.api.BootstrapService;
import com.cs324.backend.api.WorkerInfo;
import com.cs324.backend.api.WorkerService;
import com.cs324.backend.bootstrap.BootstrapServer;
import com.cs324.backend.worker.WorkerServer;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.table.AbstractTableModel;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.Font;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client GUI. Runs as its own Java process and talks to the coordinator
 * worker over RMI. Supports manual or CSV input, three job types (MAX,
 * PRIMECOUNT, PRIMESUM) and concurrent submissions from one or more
 * clients at the same time.
 *
 * <p>All RMI calls happen on a small background executor, never on the
 * Swing event thread, so the interface never freezes while a job runs.</p>
 */
public class ClientGUI extends JFrame {

    private static final String[] JOB_TYPES = {"MAX", "PRIMECOUNT", "PRIMESUM"};
    private static final String[] COLUMNS = {"Job ID", "Type", "Submitted", "Status", "Coordinator", "Result", "Error"};

    private final JTextField hostField = new JTextField("localhost", 10);
    private final JTextField bootstrapPortField = new JTextField(String.valueOf(BootstrapServer.DEFAULT_PORT), 6);
    private final JButton connectButton = new JButton("Connect");
    private final JLabel connectionStatus = new JLabel("Not connected");

    private final JComboBox<String> jobTypeCombo = new JComboBox<>(JOB_TYPES);
    private final JTextArea inputArea = new JTextArea(4, 40);
    private final JButton loadCsvButton = new JButton("Load CSV...");
    private final JButton submitButton = new JButton("Submit");
    private final JButton clearButton = new JButton("Clear");
    private final JLabel coordinatorLabel = new JLabel("Coordinator: none");

    private final JTextArea logArea = new JTextArea(8, 60);
    private final JTable taskTable = new JTable();
    private final TaskTableModel tableModel = new TaskTableModel();

    private final ExecutorService jobExecutor = Executors.newFixedThreadPool(4, runnable -> {
        Thread thread = new Thread(runnable, "client-job-executor");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicInteger jobSequence = new AtomicInteger(0);

    private volatile String bootstrapHost = "localhost";
    private volatile int bootstrapPort = BootstrapServer.DEFAULT_PORT;
    private volatile WorkerService coordinator;

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new ClientGUI().setVisible(true));
    }

    public ClientGUI() {
        super("CS324 Distributed Client");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(780, 660);
        setLocationRelativeTo(null);

        getContentPane().add(buildConnectionPanel(), BorderLayout.NORTH);
        getContentPane().add(buildCenter(), BorderLayout.CENTER);

        connectButton.addActionListener(e -> connect());
        submitButton.addActionListener(e -> submitJob());
        loadCsvButton.addActionListener(e -> loadCsv());
        clearButton.addActionListener(e -> {
            inputArea.setText("");
            log("Input cleared");
        });
        jobTypeCombo.addActionListener(e -> updateHint());

        logArea.setEditable(false);
        logArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        taskTable.setModel(tableModel);
        taskTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        taskTable.setAutoCreateRowSorter(true);

        updateHint();
        log("Client ready. Connect to the Bootstrap Node, then submit jobs (runs in the background).");
    }

    private JPanel buildConnectionPanel() {
        JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        panel.setBorder(BorderFactory.createTitledBorder("Connection"));
        panel.add(new JLabel("Bootstrap host:"));
        panel.add(hostField);
        panel.add(new JLabel("Port:"));
        panel.add(bootstrapPortField);
        panel.add(connectButton);
        panel.add(connectionStatus);
        return panel;
    }

    private JPanel buildCenter() {
        JPanel jobPanel = new JPanel(new BorderLayout());
        jobPanel.setBorder(BorderFactory.createTitledBorder("Job"));

        JPanel header = new JPanel(new FlowLayout(FlowLayout.LEFT));
        header.add(new JLabel("Job type:"));
        header.add(jobTypeCombo);
        header.add(coordinatorLabel);

        JPanel input = new JPanel(new BorderLayout(4, 4));
        input.add(new JScrollPane(inputArea), BorderLayout.CENTER);

        JPanel footer = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        footer.add(loadCsvButton);
        footer.add(clearButton);
        footer.add(submitButton);

        JPanel jobInput = new JPanel(new BorderLayout());
        jobInput.add(header, BorderLayout.NORTH);
        jobInput.add(input, BorderLayout.CENTER);
        jobInput.add(footer, BorderLayout.SOUTH);

        JPanel tablePanel = new JPanel(new BorderLayout());
        tablePanel.setBorder(BorderFactory.createTitledBorder("Tasks (concurrent submissions supported)"));
        tablePanel.add(new JScrollPane(taskTable), BorderLayout.CENTER);

        JPanel logPanel = new JPanel(new BorderLayout());
        logPanel.setBorder(BorderFactory.createTitledBorder("Log"));
        logPanel.add(new JScrollPane(logArea), BorderLayout.CENTER);

        JSplitPane bottom = new JSplitPane(JSplitPane.VERTICAL_SPLIT, tablePanel, logPanel);
        bottom.setResizeWeight(0.6);

        JPanel center = new JPanel(new BorderLayout());
        center.add(jobInput, BorderLayout.NORTH);
        center.add(bottom, BorderLayout.CENTER);
        return center;
    }

    private void updateHint() {
        String type = (String) jobTypeCombo.getSelectedItem();
        inputArea.setToolTipText("PRIMESUM".equals(type) ? "start,end  e.g. 1,1000"
                : "comma-separated integers  e.g. 12,5,99,2,17");
    }

    /** Connects to the Bootstrap Node and locates the current coordinator worker. */
    private void connect() {
        bootstrapHost = hostField.getText().trim().isEmpty() ? "localhost" : hostField.getText().trim();
        try {
            bootstrapPort = Integer.parseInt(bootstrapPortField.getText().trim());
        } catch (NumberFormatException e) {
            appendError("Bootstrap port must be a number");
            return;
        }

        connectButton.setEnabled(false);
        connectionStatus.setForeground(Color.ORANGE);
        connectionStatus.setText("Connecting...");
        jobExecutor.submit(this::doConnect);
    }

    private void doConnect() {
        try {
            Registry registry = LocateRegistry.getRegistry(bootstrapHost, bootstrapPort);
            BootstrapService bootstrap = (BootstrapService) registry.lookup(BootstrapServer.SERVICE_NAME);
            int coordinatorId = findCoordinatorId(bootstrap);
            WorkerService found = coordinatorId == WorkerService.NO_COORDINATOR
                    ? null
                    : lookupRegistered(bootstrap, coordinatorId);
            coordinator = found;
            final int elected = coordinatorId;
            SwingUtilities.invokeLater(() -> {
                connectButton.setEnabled(true);
                log("Connected to Bootstrap Node at " + bootstrapHost + ":" + bootstrapPort
                        + (elected == WorkerService.NO_COORDINATOR ? " - no coordinator elected yet"
                        : " - coordinator is worker " + elected));
                if (elected == WorkerService.NO_COORDINATOR) {
                    connectionStatus.setForeground(Color.ORANGE);
                    connectionStatus.setText("Connected - no coordinator");
                    coordinatorLabel.setText("Coordinator: none (run an election)");
                } else {
                    connectionStatus.setForeground(new Color(0, 128, 0));
                    connectionStatus.setText("Connected to " + bootstrapHost + ":" + bootstrapPort);
                    coordinatorLabel.setText("Coordinator: worker " + elected);
                }
            });
        } catch (Exception e) {
            SwingUtilities.invokeLater(() -> {
                connectButton.setEnabled(true);
                connectionStatus.setForeground(Color.RED);
                connectionStatus.setText("Connection failed");
                appendError("Could not connect: " + rootMessage(e));
            });
        }
    }

    /**
     * Asks every registered worker which coordinator it knows and returns the
     * agreed coordinator id (or {@link WorkerService#NO_COORDINATOR} if none).
     */
    private int findCoordinatorId(BootstrapService bootstrap) throws Exception {
        for (WorkerInfo info : bootstrap.getActiveWorkers()) {
            WorkerService worker = lookup(info);
            if (worker == null) {
                continue;
            }
            int candidate = worker.getCurrentCoordinatorId();
            if (candidate != WorkerService.NO_COORDINATOR) {
                return candidate;
            }
        }
        return WorkerService.NO_COORDINATOR;
    }

    private WorkerService lookupRegistered(BootstrapService bootstrap, int workerId) throws Exception {
        for (WorkerInfo info : bootstrap.getActiveWorkers()) {
            if (info.getWorkerId() == workerId) {
                WorkerService worker = lookup(info);
                if (worker != null) {
                    return worker;
                }
            }
        }
        throw new Exception("Coordinator worker " + workerId + " is not reachable");
    }

    private WorkerService lookup(WorkerInfo info) {
        try {
            Registry registry = LocateRegistry.getRegistry(info.getHost(), info.getPort());
            return (WorkerService) registry.lookup(WorkerServer.SERVICE_NAME_PREFIX + info.getWorkerId());
        } catch (Exception e) {
            System.err.println("[Client] cannot reach " + info + ": " + e.getMessage());
            return null;
        }
    }

    private void submitJob() {
        if (coordinator == null) {
            appendError("No coordinator - press Connect (an election must have run)");
            return;
        }

        String type = (String) jobTypeCombo.getSelectedItem();
        String raw = inputArea.getText().trim();
        if (raw.isEmpty()) {
            appendError("Input is empty");
            return;
        }

        final int jobId = jobSequence.incrementAndGet();
        final String submitted = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        final WorkerService target = coordinator;

        addTask(jobId, type, submitted, "Running", String.valueOf(currentCoordinatorId(target)), "-", null);

        jobExecutor.submit(() -> {
            try {
                Object result = dispatch(target, type, raw);
                updateTask(jobId, "Done", result.toString(), null);
                log("Job " + jobId + " [" + type + "] finished -> " + result);
            } catch (Exception e) {
                String message = rootMessage(e);
                updateTask(jobId, "Failed", "-", message);
                log("Job " + jobId + " [" + type + "] failed -> " + message);
            }
        });
    }

    private int currentCoordinatorId(WorkerService worker) {
        try {
            return worker.getCurrentCoordinatorId();
        } catch (Exception e) {
            return WorkerService.NO_COORDINATOR;
        }
    }

    private Object dispatch(WorkerService target, String type, String raw) throws Exception {
        switch (type) {
            case "MAX":
                return target.submitMaxJob(parseNumbers(raw));
            case "PRIMECOUNT":
                return target.submitPrimeCount(parseNumbers(raw));
            case "PRIMESUM":
                int[] range = parseRange(raw);
                return target.submitPrimeSum(range[0], range[1]);
            default:
                throw new IllegalArgumentException("Unknown job type: " + type);
        }
    }

    private void loadCsv() {
        JFileChooser chooser = new JFileChooser();
        if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) {
            return;
        }
        Path path = chooser.getSelectedFile().toPath();
        try {
            String content = Files.readString(path).trim();
            inputArea.setText(content);
            log("Loaded CSV from " + path.getFileName());
        } catch (IOException e) {
            appendError("Could not read CSV file: " + e.getMessage());
        }
    }

    /** Parses "12,5,99,2,17" into a list of integers. */
    static List<Integer> parseNumbers(String raw) {
        List<Integer> numbers = new ArrayList<>();
        for (String part : raw.split("[,\\s]+")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                numbers.add(Integer.parseInt(trimmed));
            }
        }
        if (numbers.isEmpty()) {
            throw new IllegalArgumentException("no numbers found in input");
        }
        return numbers;
    }

    /** Parses "1,1000" into a start/end range. */
    static int[] parseRange(String raw) {
        List<Integer> parts = parseParts(raw);
        if (parts.size() < 2) {
            throw new IllegalArgumentException("PRIMESUM needs two values: start,end");
        }
        return new int[]{parts.get(0), parts.get(1)};
    }

    private static List<Integer> parseParts(String raw) {
        List<Integer> parts = new ArrayList<>();
        for (String part : raw.split("[,\\s]+")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                parts.add(Integer.parseInt(trimmed));
            }
        }
        return parts;
    }

    private String rootMessage(Throwable t) {
        Throwable current = t;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        String message = current.getMessage();
        return message == null ? current.getClass().getSimpleName() : message;
    }

    private void addTask(int jobId, String type, String submitted, String status, String coordinator, String result, String error) {
        SwingUtilities.invokeLater(() -> tableModel.addRow(jobId, type, submitted, status, coordinator, result, error));
    }

    private void updateTask(int jobId, String status, String result, String error) {
        SwingUtilities.invokeLater(() -> tableModel.updateRow(jobId, status, result, error));
    }

    private void log(String message) {
        logArea.append("[" + LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "] " + message + "\n");
        logArea.setCaretPosition(logArea.getDocument().getLength());
    }

    private void appendError(String message) {
        log("ERROR: " + message);
        JOptionPane.showMessageDialog(this, message, "Error", JOptionPane.ERROR_MESSAGE);
    }

    /** Non-editable table model for submitted tasks. */
    private static final class TaskTableModel extends AbstractTableModel {
        private final List<Object[]> rows = new ArrayList<>();

        @Override
        public int getRowCount() {
            return rows.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMNS.length;
        }

        @Override
        public String getColumnName(int column) {
            return COLUMNS[column];
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            return rows.get(rowIndex)[columnIndex];
        }

        @Override
        public boolean isCellEditable(int rowIndex, int columnIndex) {
            return false;
        }

        void addRow(int jobId, String type, String submitted, String status, String coordinator, String result, String error) {
            rows.add(new Object[]{jobId, type, submitted, status, coordinator, result, error});
            fireTableRowsInserted(rows.size() - 1, rows.size() - 1);
        }

        void updateRow(int jobId, String status, String result, String error) {
            for (int i = 0; i < rows.size(); i++) {
                if ((Integer) rows.get(i)[0] == jobId) {
                    rows.get(i)[3] = status;
                    rows.get(i)[5] = result;
                    rows.get(i)[6] = error;
                    fireTableRowsUpdated(i, i);
                    return;
                }
            }
        }
    }
}