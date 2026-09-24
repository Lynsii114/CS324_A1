package com.cs324.backend.gui;

import com.cs324.backend.api.BootstrapService;
import com.cs324.backend.api.WorkerService;
import com.cs324.backend.bootstrap.BootstrapServer;
import com.cs324.backend.bootstrap.BootstrapServiceImpl;
import com.cs324.backend.worker.WorkerClusterConfig;
import com.cs324.backend.worker.WorkerClusterLauncher;
import com.cs324.backend.worker.WorkerServer;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComponent;
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
import javax.swing.Timer;
import javax.swing.table.AbstractTableModel;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.Font;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Server-side GUI. From here the Bootstrap Node and the four worker
 * processes are started, elections can be triggered, and the live cluster
 * state (reachability, JAC, coordinator agreement) is shown in a table so
 * an operator can confirm the network is online before clients launch.
 *
 * <p>Heavy actions (bootstrap start, worker launch, elections) run on a
 * background executor; the UI is only touched through {@code invokeLater}.</p>
 */
public class ServerGUI extends JFrame {

    private static final String[] COLUMNS = {"Worker", "Port", "Reachable", "JAC", "JobsThisTerm", "Coordinator", "Neighbours", "Leaderman"};

    private final JTextField hostField = new JTextField(WorkerClusterConfig.DEFAULT_HOST, 10);
    private final JTextField bootstrapPortField = new JTextField(String.valueOf(BootstrapServer.DEFAULT_PORT), 6);
    private final JLabel bootstrapStatus = new JLabel("Bootstrap: not started");
    private final JLabel coordinatorStatus = new JLabel("Coordinator: none");

    private final JButton startBootstrapButton = new JButton("Start Bootstrap");
    private final JButton startWorkersButton = new JButton("Start Workers");
    private final JButton stopWorkersButton = new JButton("Stop Workers");
    private final JButton electionButton = new JButton("Run Election");
    private final JButton resetButton = new JButton("Reset Coordinators");
    private final JButton refreshButton = new JButton("Refresh");

    private final JTextArea logArea = new JTextArea(10, 60);
    private final JTable statusTable = new JTable();
    private final StatusTableModel tableModel = new StatusTableModel();

    private final ExecutorService executor = Executors.newFixedThreadPool(2, runnable -> {
        Thread thread = new Thread(runnable, "server-gui-worker");
        thread.setDaemon(true);
        return thread;
    });

    private volatile String bootstrapHost = WorkerClusterConfig.DEFAULT_HOST;
    private volatile int bootstrapPort = BootstrapServer.DEFAULT_PORT;

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new ServerGUI().setVisible(true));
    }

    public ServerGUI() {
        super("CS324 Cluster Server Manager");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(820, 620);
        setLocationRelativeTo(null);

        getContentPane().add(buildControlPanel(), BorderLayout.NORTH);
        getContentPane().add(buildCenter(), BorderLayout.CENTER);

        startBootstrapButton.addActionListener(e -> startBootstrap());
        startWorkersButton.addActionListener(e -> startWorkers());
        stopWorkersButton.addActionListener(e -> stopWorkers());
        electionButton.addActionListener(e -> runElection());
        resetButton.addActionListener(e -> resetCoordinators());
        refreshButton.addActionListener(e -> refreshStatus());

        logArea.setEditable(false);
        logArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        statusTable.setModel(tableModel);
        statusTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

        log("Server manager ready. Start the Bootstrap Node, then start the workers.");

        Timer timer = new Timer(3000, e -> refreshBackground());
        timer.start();
    }

    private JPanel buildControlPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Cluster Management"));

        JPanel settings = new JPanel(new FlowLayout(FlowLayout.LEFT));
        settings.add(new JLabel("Host:"));
        settings.add(hostField);
        settings.add(new JLabel("Bootstrap port:"));
        settings.add(bootstrapPortField);

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.LEFT));
        actions.add(startBootstrapButton);
        actions.add(startWorkersButton);
        actions.add(stopWorkersButton);
        actions.add(electionButton);
        actions.add(resetButton);
        actions.add(refreshButton);

        JPanel status = new JPanel(new FlowLayout(FlowLayout.LEFT));
        status.add(bootstrapStatus);
        status.add(coordinatorStatus);

        panel.add(settings, BorderLayout.NORTH);
        panel.add(actions, BorderLayout.CENTER);
        panel.add(status, BorderLayout.SOUTH);
        return panel;
    }

    private JComponent buildCenter() {
        JPanel tablePanel = new JPanel(new BorderLayout());
        tablePanel.setBorder(BorderFactory.createTitledBorder("Cluster State (auto-refreshes)"));
        tablePanel.add(new JScrollPane(statusTable), BorderLayout.CENTER);

        JPanel logPanel = new JPanel(new BorderLayout());
        logPanel.setBorder(BorderFactory.createTitledBorder("Log"));
        logPanel.add(new JScrollPane(logArea), BorderLayout.CENTER);

        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, tablePanel, logPanel);
        split.setResizeWeight(0.6);
        return split;
    }

    private void startBootstrap() {
        readSettings();
        startBootstrapButton.setEnabled(false);
        executor.submit(() -> {
            try {
                Registry registry;
                try {
                    registry = LocateRegistry.createRegistry(bootstrapPort);
                } catch (ExportException alreadyRunning) {
                    registry = LocateRegistry.getRegistry(bootstrapPort);
                }
                try {
                    registry.lookup(BootstrapServer.SERVICE_NAME);
                    log("Bootstrap Node already running on port " + bootstrapPort);
                } catch (Exception notBound) {
                    registry.rebind(BootstrapServer.SERVICE_NAME, new BootstrapServiceImpl());
                    log("Bootstrap Node started and bound on port " + bootstrapPort);
                }
                SwingUtilities.invokeLater(() -> {
                    bootstrapStatus.setForeground(new Color(0, 128, 0));
                    bootstrapStatus.setText("Bootstrap: running on " + bootstrapHost + ":" + bootstrapPort);
                    startBootstrapButton.setEnabled(true);
                });
            } catch (Exception e) {
                SwingUtilities.invokeLater(() -> {
                    startBootstrapButton.setEnabled(true);
                    appendError("Bootstrap Node failed to start: " + rootMessage(e));
                });
            }
        });
    }

    private void startWorkers() {
        readSettings();
        startWorkersButton.setEnabled(false);
        executor.submit(() -> {
            try {
                log("Launching " + WorkerClusterConfig.WORKER_COUNT + " worker processes (this can take a few seconds)...");
                WorkerClusterLauncher.start(bootstrapHost, bootstrapPort);
                SwingUtilities.invokeLater(() -> {
                    log("Worker cluster launch finished - refreshing state");
                    startWorkersButton.setEnabled(true);
                    refreshBackground();
                });
            } catch (Exception e) {
                SwingUtilities.invokeLater(() -> {
                    startWorkersButton.setEnabled(true);
                    appendError("Could not start workers: " + rootMessage(e));
                });
            }
        });
    }

    private void stopWorkers() {
        executor.submit(() -> {
            WorkerClusterLauncher.stop();
            log("Worker processes stopped - refreshing state");
            refreshBackground();
        });
    }

    private void runElection() {
        readSettings();
        electionButton.setEnabled(false);
        executor.submit(() -> {
            try {
                WorkerService initiator = lookupWorker(WorkerClusterConfig.FIRST_WORKER_ID);
                log("Initiating leader election from worker " + WorkerClusterConfig.FIRST_WORKER_ID + "...");
                String result = initiator.initiateElection();
                log("Election result: " + result);
                SwingUtilities.invokeLater(() -> {
                    electionButton.setEnabled(true);
                    refreshBackground();
                });
            } catch (Exception e) {
                SwingUtilities.invokeLater(() -> {
                    electionButton.setEnabled(true);
                    appendError("Election failed: " + rootMessage(e));
                });
            }
        });
    }

    private void resetCoordinators() {
        readSettings();
        executor.submit(() -> {
            int cleared = 0;
            for (int workerId : WorkerClusterConfig.workerIds()) {
                try {
                    WorkerService worker = lookupWorker(workerId);
                    worker.setCurrentCoordinatorId(WorkerService.NO_COORDINATOR);
                    cleared++;
                } catch (Exception e) {
                    log("Worker " + workerId + " not reachable while resetting: " + e.getMessage());
                }
            }
            log("Reset coordinators on " + cleared + " worker(s) - run a new election to re-elect");
            refreshBackground();
        });
    }

    private void refreshStatus() {
        executor.submit(this::refreshBackground);
    }

    private void refreshBackground() {
        List<Object[]> rows = new ArrayList<>();
        int reachable = 0;
        java.util.Set<Integer> coordinators = new java.util.TreeSet<>();

        for (int workerId : WorkerClusterConfig.workerIds()) {
            try {
                WorkerService worker = lookupWorker(workerId);
                int coordinator = worker.getCurrentCoordinatorId();
                coordinators.add(coordinator);
                rows.add(new Object[]{
                        worker.getWorkerId(),
                        WorkerClusterConfig.portFor(workerId),
                        "yes",
                        worker.getJobAllocationCounter(),
                        worker.getJobsThisTerm(),
                        coordinator == WorkerService.NO_COORDINATOR ? "none" : coordinator,
                        worker.getNeighbours().size(),
                        worker.getLeaderman()});
                reachable++;
            } catch (Exception e) {
                rows.add(new Object[]{workerId, WorkerClusterConfig.portFor(workerId), "no", "-", "-", "-", "-", "-"});
            }
        }

        final int reachableCount = reachable;
        final int total = WorkerClusterConfig.WORKER_COUNT;
        final boolean agreed = coordinators.size() == 1 && !coordinators.contains(WorkerService.NO_COORDINATOR);
        final String coordinatorText = reachableCount == 0 ? "none"
                : agreed ? "worker " + coordinators.iterator().next()
                : reachableCount == total && coordinators.contains(WorkerService.NO_COORDINATOR)
                ? "none elected yet - press Run Election"
                : "mismatch across workers";

        SwingUtilities.invokeLater(() -> {
            tableModel.setRows(rows);
            coordinatorStatus.setText("Network: " + reachableCount + "/" + total + " workers reachable | Coordinator: " + coordinatorText);
            coordinatorStatus.setForeground(agreed ? new Color(0, 128, 0) : Color.ORANGE);
        });
    }

    private void readSettings() {
        String host = hostField.getText().trim();
        bootstrapHost = host.isEmpty() ? WorkerClusterConfig.DEFAULT_HOST : host;
        try {
            bootstrapPort = Integer.parseInt(bootstrapPortField.getText().trim());
        } catch (NumberFormatException e) {
            appendError("Bootstrap port must be a number");
        }
    }

    private WorkerService lookupWorker(int workerId) throws Exception {
        Registry registry = LocateRegistry.getRegistry(bootstrapHost, WorkerClusterConfig.portFor(workerId));
        return (WorkerService) registry.lookup(WorkerServer.SERVICE_NAME_PREFIX + workerId);
    }

    private String rootMessage(Throwable t) {
        Throwable current = t;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        String message = current.getMessage();
        return message == null ? current.getClass().getSimpleName() : message;
    }

    private void log(String message) {
        logArea.append("[" + LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "] " + message + "\n");
        logArea.setCaretPosition(logArea.getDocument().getLength());
    }

    private void appendError(String message) {
        log("ERROR: " + message);
        JOptionPane.showMessageDialog(this, message, "Error", JOptionPane.ERROR_MESSAGE);
    }

    private static final class StatusTableModel extends AbstractTableModel {
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

        void setRows(List<Object[]> rows) {
            this.rows.clear();
            this.rows.addAll(rows);
            fireTableDataChanged();
        }
    }
}