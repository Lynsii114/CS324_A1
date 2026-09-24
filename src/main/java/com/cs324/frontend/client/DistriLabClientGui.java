package com.cs324.frontend.client;

import com.cs324.backend.api.WorkerService;
import com.cs324.backend.worker.WorkerClusterConfig;
import com.cs324.backend.worker.WorkerServer;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.WindowConstants;
import javax.swing.border.TitledBorder;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Swing frontend for submitting DistriLab jobs to the elected coordinator.
 */
public class DistriLabClientGui extends JFrame {
    private static final long serialVersionUID = 1L;
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final Color BACKGROUND = new Color(14, 18, 27);
    private static final Color PANEL = new Color(24, 30, 43);
    private static final Color PANEL_ALT = new Color(18, 23, 34);
    private static final Color FIELD = new Color(10, 14, 22);
    private static final Color BORDER = new Color(55, 65, 81);
    private static final Color TEXT = new Color(232, 238, 247);
    private static final Color MUTED_TEXT = new Color(154, 165, 181);
    private static final Color ACCENT = new Color(45, 212, 191);
    private static final Color ACCENT_DARK = new Color(15, 118, 110);
    private static final Color SUCCESS = new Color(34, 197, 94);
    private static final Color DANGER = new Color(248, 113, 113);

    private final JTextField hostField = new JTextField(WorkerClusterConfig.DEFAULT_HOST, 14);
    private final JTextField coordinatorIdField = new JTextField(String.valueOf(WorkerClusterConfig.LAST_WORKER_ID), 4);
    private final JTextField coordinatorPortField = new JTextField(
            String.valueOf(WorkerClusterConfig.portFor(WorkerClusterConfig.LAST_WORKER_ID)), 6);
    private final JComboBox<JobType> jobTypeCombo = new JComboBox<>(JobType.values());
    private final JTextArea dataArea = new JTextArea("12, 7, 44, 3, 91, 18, 55, 6, 72, 10, 29", 7, 48);
    private final JTextArea resultArea = new JTextArea(11, 56);
    private final JLabel statusLabel = new JLabel("Ready");
    private final JButton submitButton = new JButton("Submit Job");
    private final JButton refreshNodesButton = new JButton("Refresh Nodes");
    private final List<JLabel> nodeLabels = new ArrayList<>();

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            setDarkLookAndFeel();
            DistriLabClientGui gui = new DistriLabClientGui();
            gui.setVisible(true);
        });
    }

    public DistriLabClientGui() {
        super("DistriLab Client");
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setMinimumSize(new Dimension(920, 680));
        setLocationByPlatform(true);

        getContentPane().setBackground(BACKGROUND);
        resultArea.setEditable(false);
        dataArea.setLineWrap(true);
        dataArea.setWrapStyleWord(true);
        resultArea.setLineWrap(true);
        resultArea.setWrapStyleWord(true);
        styleField(hostField);
        styleField(coordinatorIdField);
        styleField(coordinatorPortField);
        styleTextArea(dataArea);
        styleTextArea(resultArea);
        styleComboBox(jobTypeCombo);
        stylePrimaryButton(submitButton);
        styleSecondaryButton(refreshNodesButton);
        styleStatusLabel(statusLabel);

        submitButton.addActionListener(event -> submitJob());
        refreshNodesButton.addActionListener(event -> refreshNodeStatus());
        coordinatorIdField.addActionListener(event -> updatePortFromCoordinatorId());

        JPanel content = new JPanel(new BorderLayout(16, 16));
        content.setBackground(BACKGROUND);
        content.setBorder(BorderFactory.createEmptyBorder(18, 20, 20, 20));
        content.add(headerPanel(), BorderLayout.NORTH);
        content.add(darkPanelScrollPane(workspacePanel()), BorderLayout.CENTER);
        setContentPane(content);
        pack();
        refreshNodeStatus();
    }

    private JPanel headerPanel() {
        JPanel wrapper = new JPanel(new BorderLayout(0, 14));
        wrapper.setOpaque(false);

        JPanel titlePanel = new JPanel(new BorderLayout(8, 4));
        titlePanel.setOpaque(false);
        JLabel title = new JLabel("DistriLab Client");
        title.setForeground(TEXT);
        title.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 28));
        JLabel subtitle = new JLabel("Submit distributed jobs to the elected RMI coordinator");
        subtitle.setForeground(MUTED_TEXT);
        subtitle.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 13));
        titlePanel.add(title, BorderLayout.NORTH);
        titlePanel.add(subtitle, BorderLayout.SOUTH);

        wrapper.add(titlePanel, BorderLayout.NORTH);
        wrapper.add(connectionPanel(), BorderLayout.SOUTH);
        return wrapper;
    }

    private JPanel workspacePanel() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setOpaque(false);

        JPanel cluster = clusterPanel();
        cluster.setAlignmentX(LEFT_ALIGNMENT);
        JPanel job = jobPanel();
        job.setAlignmentX(LEFT_ALIGNMENT);
        JPanel results = resultsPanel();
        results.setAlignmentX(LEFT_ALIGNMENT);

        panel.add(cluster);
        panel.add(Box.createVerticalStrut(14));
        panel.add(job);
        panel.add(Box.createVerticalStrut(14));
        panel.add(results);
        return panel;
    }

    private JPanel clusterPanel() {
        JPanel panel = cardPanel("Six Worker Nodes");
        panel.setLayout(new BorderLayout(10, 12));

        JPanel topRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        topRow.setOpaque(false);
        JLabel note = new JLabel("Configured active worker cluster: six RMI nodes, lowest JAC wins, highest ID breaks ties");
        styleMutedLabel(note);
        topRow.add(note);
        topRow.add(Box.createHorizontalStrut(14));
        topRow.add(refreshNodesButton);

        JPanel nodes = new JPanel(new GridLayout(2, 3, 10, 10));
        nodes.setOpaque(false);
        nodes.setPreferredSize(new Dimension(840, 250));
        nodeLabels.clear();
        for (int workerId : WorkerClusterConfig.workerIds()) {
            JLabel nodeLabel = new JLabel();
            nodeLabel.setOpaque(true);
            nodeLabel.setVerticalAlignment(JLabel.TOP);
            nodeLabel.setBorder(BorderFactory.createCompoundBorder(
                    BorderFactory.createLineBorder(BORDER),
                    BorderFactory.createEmptyBorder(11, 12, 11, 12)));
            updateNodeLabel(nodeLabel, new NodeState(workerId, WorkerClusterConfig.portFor(workerId),
                    "Not checked", "-", "-", "-", "-", MUTED_TEXT));
            nodeLabels.add(nodeLabel);
            nodes.add(nodeLabel);
        }

        panel.add(topRow, BorderLayout.NORTH);
        panel.add(nodes, BorderLayout.CENTER);
        return panel;
    }

    private JPanel connectionPanel() {
        JPanel panel = cardPanel("Coordinator RMI");
        panel.setLayout(new GridBagLayout());
        GridBagConstraints gbc = baseConstraints();

        addLabeledField(panel, "Host", hostField, gbc, 0);
        addLabeledField(panel, "Coordinator ID", coordinatorIdField, gbc, 1);
        addLabeledField(panel, "Port", coordinatorPortField, gbc, 2);

        JButton useDefaultPortButton = new JButton("Use ID Port");
        styleSecondaryButton(useDefaultPortButton);
        useDefaultPortButton.addActionListener(event -> updatePortFromCoordinatorId());
        gbc.gridx = 6;
        gbc.gridy = 0;
        gbc.gridheight = 2;
        gbc.weightx = 1.0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(10, 10, 10, 10);
        panel.add(useDefaultPortButton, gbc);
        return panel;
    }

    private JPanel jobPanel() {
        JPanel panel = cardPanel("Job Setup");
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));

        JPanel controls = new JPanel(new FlowLayout(FlowLayout.LEFT, 12, 4));
        controls.setOpaque(false);
        JLabel typeLabel = new JLabel("Job Type");
        styleLabel(typeLabel);
        controls.add(typeLabel);
        controls.add(jobTypeCombo);
        controls.add(submitButton);

        panel.add(controls);
        panel.add(Box.createVerticalStrut(12));
        JLabel inputLabel = new JLabel("Input integers, separated by commas, spaces, or new lines");
        inputLabel.setAlignmentX(LEFT_ALIGNMENT);
        styleMutedLabel(inputLabel);
        panel.add(inputLabel);
        panel.add(Box.createVerticalStrut(8));
        JScrollPane scrollPane = darkScrollPane(dataArea);
        scrollPane.setAlignmentX(LEFT_ALIGNMENT);
        panel.add(scrollPane);
        return panel;
    }

    private JPanel resultsPanel() {
        JPanel panel = cardPanel("Results");
        panel.setLayout(new BorderLayout(8, 10));
        panel.setPreferredSize(new Dimension(840, 160));
        panel.setMaximumSize(new Dimension(Integer.MAX_VALUE, 180));
        panel.add(darkScrollPane(resultArea), BorderLayout.CENTER);
        panel.add(statusLabel, BorderLayout.SOUTH);
        return panel;
    }

    private void submitJob() {
        JobRequest request;
        try {
            request = readJobRequest();
        } catch (IllegalArgumentException e) {
            JOptionPane.showMessageDialog(this, e.getMessage(), "Invalid Job", JOptionPane.WARNING_MESSAGE);
            return;
        }

        submitButton.setEnabled(false);
        statusLabel.setText("Running " + request.jobType + " on coordinator " + request.coordinatorId + "...");
        appendResult("[" + now() + "] Sending " + request.jobType + " job to " + request.host
                + ":" + request.port + " with " + request.numbers.size() + " values");

        new JobWorker(request).execute();
    }

    private void refreshNodeStatus() {
        String host = hostField.getText().trim();
        if (host.isEmpty()) {
            host = WorkerClusterConfig.DEFAULT_HOST;
        }

        refreshNodesButton.setEnabled(false);
        statusLabel.setText("Checking six worker nodes...");
        for (int i = 0; i < nodeLabels.size(); i++) {
            int workerId = WorkerClusterConfig.FIRST_WORKER_ID + i;
            updateNodeLabel(nodeLabels.get(i), new NodeState(workerId, WorkerClusterConfig.portFor(workerId),
                    "Checking", "-", "-", "-", "-", MUTED_TEXT));
        }

        new NodeStatusWorker(host).execute();
    }

    private JobRequest readJobRequest() {
        String host = hostField.getText().trim();
        if (host.isEmpty()) {
            throw new IllegalArgumentException("Host is required.");
        }

        int coordinatorId = parsePositiveInt(coordinatorIdField.getText(), "Coordinator ID");
        int port = parsePositiveInt(coordinatorPortField.getText(), "Coordinator port");
        JobType jobType = (JobType) jobTypeCombo.getSelectedItem();
        List<Integer> numbers = parseNumbers(dataArea.getText());
        if (numbers.isEmpty()) {
            throw new IllegalArgumentException("Enter at least one integer.");
        }
        return new JobRequest(host, port, coordinatorId, jobType, numbers);
    }

    private void updatePortFromCoordinatorId() {
        try {
            int workerId = parsePositiveInt(coordinatorIdField.getText(), "Coordinator ID");
            coordinatorPortField.setText(String.valueOf(WorkerClusterConfig.portFor(workerId)));
        } catch (IllegalArgumentException e) {
            JOptionPane.showMessageDialog(this, e.getMessage(), "Invalid Coordinator ID", JOptionPane.WARNING_MESSAGE);
        }
    }

    private int parsePositiveInt(String value, String label) {
        try {
            int parsed = Integer.parseInt(value.trim());
            if (parsed <= 0) {
                throw new NumberFormatException("not positive");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(label + " must be a positive integer.");
        }
    }

    private List<Integer> parseNumbers(String rawInput) {
        List<Integer> numbers = new ArrayList<>();
        for (String part : rawInput.split("[,\\s]+")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                try {
                    numbers.add(Integer.parseInt(trimmed));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid integer: " + trimmed);
                }
            }
        }
        return numbers;
    }

    private void appendResult(String line) {
        resultArea.append(line + System.lineSeparator());
        resultArea.setCaretPosition(resultArea.getDocument().getLength());
    }

    private String now() {
        return LocalTime.now().format(TIME_FORMAT);
    }

    private void updateNodeLabel(JLabel label, NodeState state) {
        label.setBackground(PANEL_ALT);
        label.setForeground(TEXT);
        label.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
        Color borderColor = "Coordinator".equals(state.role) ? ACCENT : BORDER;
        label.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(borderColor),
                BorderFactory.createEmptyBorder(11, 12, 11, 12)));
        label.setText("<html><body style='width: 190px'>"
                + "<b style='font-size: 14px'>Worker " + state.workerId + "</b>"
                + " <span style='color: " + hex(state.statusColor) + "'>" + state.status + "</span><br>"
                + "<span style='color: " + hex(MUTED_TEXT) + "'>Port " + state.port + "</span><br>"
                + "<span style='color: " + hex(TEXT) + "'>Role: " + state.role + "</span><br>"
                + "<span style='color: " + hex(MUTED_TEXT) + "'>Coordinator: " + state.coordinator + "</span><br>"
                + "<span style='color: " + hex(MUTED_TEXT) + "'>JAC: " + state.jac + "</span><br>"
                + "<span style='color: " + hex(MUTED_TEXT) + "'>leaderman: " + state.leaderman + "</span>"
                + "</body></html>");
    }

    private String formatCoordinator(int coordinatorId) {
        return coordinatorId == WorkerService.NO_COORDINATOR ? "none" : String.valueOf(coordinatorId);
    }

    private String hex(Color color) {
        return String.format("#%02x%02x%02x", color.getRed(), color.getGreen(), color.getBlue());
    }

    private static void setDarkLookAndFeel() {
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception ignored) {
            // Swing will fall back to the default look and feel.
        }
        UIManager.put("OptionPane.background", PANEL);
        UIManager.put("Panel.background", PANEL);
        UIManager.put("OptionPane.messageForeground", TEXT);
        UIManager.put("Button.focus", ACCENT_DARK);
    }

    private JPanel cardPanel(String title) {
        JPanel panel = new JPanel();
        panel.setBackground(PANEL);
        TitledBorder titledBorder = BorderFactory.createTitledBorder(
                BorderFactory.createLineBorder(BORDER), title);
        titledBorder.setTitleColor(TEXT);
        titledBorder.setTitleFont(new Font(Font.SANS_SERIF, Font.BOLD, 15));
        panel.setBorder(BorderFactory.createCompoundBorder(
                titledBorder,
                BorderFactory.createEmptyBorder(10, 14, 14, 14)));
        return panel;
    }

    private GridBagConstraints baseConstraints() {
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(2, 0, 3, 8);
        return gbc;
    }

    private void addLabeledField(JPanel panel, String labelText, JTextField field, GridBagConstraints gbc, int column) {
        JLabel label = new JLabel(labelText);
        styleMutedLabel(label);
        gbc.gridx = column * 2;
        gbc.gridy = 0;
        gbc.weightx = 0.0;
        gbc.fill = GridBagConstraints.NONE;
        panel.add(label, gbc);

        gbc.gridx = column * 2;
        gbc.gridy = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = labelText.equals("Host") ? 0.4 : 0.0;
        panel.add(field, gbc);
    }

    private JScrollPane darkScrollPane(JTextArea textArea) {
        JScrollPane scrollPane = new JScrollPane(textArea);
        scrollPane.setBorder(BorderFactory.createLineBorder(BORDER));
        scrollPane.getViewport().setBackground(FIELD);
        return scrollPane;
    }

    private JScrollPane darkPanelScrollPane(JPanel panel) {
        JScrollPane scrollPane = new JScrollPane(panel);
        scrollPane.setBorder(BorderFactory.createEmptyBorder());
        scrollPane.getViewport().setBackground(BACKGROUND);
        scrollPane.setBackground(BACKGROUND);
        scrollPane.getVerticalScrollBar().setUnitIncrement(16);
        return scrollPane;
    }

    private void styleLabel(JLabel label) {
        label.setForeground(TEXT);
        label.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 13));
    }

    private void styleMutedLabel(JLabel label) {
        label.setForeground(MUTED_TEXT);
        label.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
    }

    private void styleStatusLabel(JLabel label) {
        label.setForeground(ACCENT);
        label.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 12));
    }

    private void styleField(JTextField field) {
        field.setBackground(FIELD);
        field.setForeground(TEXT);
        field.setCaretColor(ACCENT);
        field.setSelectionColor(ACCENT_DARK);
        field.setSelectedTextColor(Color.WHITE);
        field.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 13));
        field.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(BORDER),
                BorderFactory.createEmptyBorder(7, 9, 7, 9)));
    }

    private void styleTextArea(JTextArea textArea) {
        textArea.setBackground(FIELD);
        textArea.setForeground(TEXT);
        textArea.setCaretColor(ACCENT);
        textArea.setSelectionColor(ACCENT_DARK);
        textArea.setSelectedTextColor(Color.WHITE);
        textArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
        textArea.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
    }

    private void styleComboBox(JComboBox<JobType> comboBox) {
        comboBox.setBackground(PANEL_ALT);
        comboBox.setForeground(TEXT);
        comboBox.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 13));
    }

    private void stylePrimaryButton(JButton button) {
        button.setBackground(ACCENT_DARK);
        button.setForeground(Color.WHITE);
        button.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 13));
        button.setFocusPainted(false);
        button.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(ACCENT),
                BorderFactory.createEmptyBorder(8, 16, 8, 16)));
    }

    private void styleSecondaryButton(JButton button) {
        button.setBackground(PANEL_ALT);
        button.setForeground(TEXT);
        button.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 12));
        button.setFocusPainted(false);
        button.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(BORDER),
                BorderFactory.createEmptyBorder(7, 12, 7, 12)));
    }

    private enum JobType {
        MAX,
        PRIMESUM,
        PRIMECOUNT
    }

    private record JobRequest(String host, int port, int coordinatorId, JobType jobType, List<Integer> numbers) {
    }

    private record NodeState(int workerId, int port, String status, String role, String coordinator, String jac,
                             String leaderman, Color statusColor) {
    }

    private final class JobWorker extends SwingWorker<String, Void> {
        private final JobRequest request;

        private JobWorker(JobRequest request) {
            this.request = request;
        }

        @Override
        protected String doInBackground() throws Exception {
            Registry registry = LocateRegistry.getRegistry(request.host, request.port);
            WorkerService coordinator = (WorkerService) registry.lookup(
                    WorkerServer.SERVICE_NAME_PREFIX + request.coordinatorId);

            return switch (request.jobType) {
                case MAX -> String.valueOf(coordinator.submitMaxJob(request.numbers));
                case PRIMESUM -> String.valueOf(coordinator.submitPrimeSumJob(request.numbers));
                case PRIMECOUNT -> String.valueOf(coordinator.submitPrimeCountJob(request.numbers));
            };
        }

        @Override
        protected void done() {
            try {
                String result = get();
                appendResult("[" + now() + "] " + request.jobType + " result: " + result);
                statusLabel.setText("Completed " + request.jobType);
            } catch (Exception e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                appendResult("[" + now() + "] " + request.jobType + " failed: " + cause.getMessage());
                statusLabel.setText("Job failed");
                JOptionPane.showMessageDialog(DistriLabClientGui.this,
                        cause.getMessage(), "Job Failed", JOptionPane.ERROR_MESSAGE);
            } finally {
                submitButton.setEnabled(true);
            }
        }
    }

    private final class NodeStatusWorker extends SwingWorker<List<NodeState>, Void> {
        private final String host;

        private NodeStatusWorker(String host) {
            this.host = host;
        }

        @Override
        protected List<NodeState> doInBackground() {
            List<NodeState> states = new ArrayList<>();
            for (int workerId : WorkerClusterConfig.workerIds()) {
                int port = WorkerClusterConfig.portFor(workerId);
                try {
                    Registry registry = LocateRegistry.getRegistry(host, port);
                    WorkerService worker = (WorkerService) registry.lookup(
                            WorkerServer.SERVICE_NAME_PREFIX + workerId);
                    int coordinator = worker.getCurrentCoordinatorId();
                    int jac = worker.getJobAllocationCounter();
                    String role = coordinator == workerId ? "Coordinator" : "Worker";
                    states.add(new NodeState(workerId, port, "Online", role, formatCoordinator(coordinator),
                            String.valueOf(jac), worker.getLeaderman(), SUCCESS));
                } catch (Exception e) {
                    states.add(new NodeState(workerId, port, "Offline", "-", "-", "-", "-", DANGER));
                }
            }
            return states;
        }

        @Override
        protected void done() {
            try {
                List<NodeState> states = get();
                int online = 0;
                for (int i = 0; i < states.size(); i++) {
                    NodeState state = states.get(i);
                    if ("Online".equals(state.status)) {
                        online++;
                    }
                    updateNodeLabel(nodeLabels.get(i), state);
                }
                statusLabel.setText(online + "/6 worker nodes online");
            } catch (Exception e) {
                statusLabel.setText("Could not refresh worker nodes");
            } finally {
                refreshNodesButton.setEnabled(true);
            }
        }
    }
}
