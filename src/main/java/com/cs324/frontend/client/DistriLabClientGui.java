package com.cs324.frontend.client;

import com.formdev.flatlaf.FlatLightLaf;
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
import javax.swing.Timer;
import javax.swing.UIManager;
import javax.swing.WindowConstants;
import javax.swing.border.TitledBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Swing frontend for submitting DistriLab jobs to the elected coordinator.
 */
public class DistriLabClientGui extends JFrame {
    private static final long serialVersionUID = 1L;
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int MAX_JOBS_PER_TERM = 5;
    private static final int MIN_INPUT_VALUES = 6;
    private static final int MAX_INPUT_VALUES = 15;
    private static final Color BACKGROUND = new Color(244, 247, 251);
    private static final Color PANEL = Color.WHITE;
    private static final Color PANEL_ALT = new Color(248, 250, 252);
    private static final Color FIELD = Color.WHITE;
    private static final Color BORDER = new Color(226, 232, 240);
    private static final Color TEXT = new Color(15, 23, 42);
    private static final Color MUTED_TEXT = new Color(100, 116, 139);
    private static final Color ACCENT = new Color(14, 116, 144);
    private static final Color ACCENT_DARK = new Color(8, 93, 117);
    private static final Color SUCCESS = new Color(5, 150, 105);
    private static final Color DANGER = new Color(220, 38, 38);
    private static final Color LEADER = new Color(202, 138, 4);

    private final JTextField hostField = new JTextField(WorkerClusterConfig.DEFAULT_HOST, 14);
    private final JTextField coordinatorIdField = new JTextField(String.valueOf(WorkerClusterConfig.LAST_WORKER_ID), 4);
    private final JTextField coordinatorPortField = new JTextField(
            String.valueOf(WorkerClusterConfig.portFor(WorkerClusterConfig.LAST_WORKER_ID)), 6);
    private final JComboBox<JobType> jobTypeCombo = new JComboBox<>(JobType.values());
    private final JTextArea dataArea = new JTextArea("", 4, 48);
    private final JTextArea resultArea = new JTextArea(10, 56);
    private final JLabel statusLabel = new JLabel("Ready");
    private final JButton submitButton = new JButton("Submit Job");
    private final JButton startElectionButton = new JButton("Start Election");
    private final JButton refreshNodesButton = new JButton("Refresh System");
    private final JButton clearInputButton = new JButton("Clear Input");
    private final JButton exitButton = new JButton("Exit");
    private final Timer systemRefreshTimer;
    private final String clientName;
    private final JLabel activeWorkersValue = new JLabel("0");
    private final JLabel leaderValue = new JLabel("none");
    private final JLabel jobsThisTermValue = new JLabel("0 / " + MAX_JOBS_PER_TERM);
    private final List<NodeCard> nodeCards = new ArrayList<>();
    private final Set<Integer> startedWorkerIds = new HashSet<>();
    private int jobsThisTerm = 0;
    private boolean jobInProgress = false;
    private boolean electionInProgress = false;
    private JobRequest pendingJobRequest;

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            setModernLookAndFeel();
            String clientName = args.length > 0 ? args[0] : "Client";
            DistriLabClientGui gui = new DistriLabClientGui(clientName);
            gui.setVisible(true);
        });
    }

    public DistriLabClientGui() {
        this("Client");
    }

    public DistriLabClientGui(String clientName) {
        super("DistriLab Client");
        this.clientName = clientName;
        setTitle("DistriLab " + clientName);
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setMinimumSize(new Dimension(520, 420));
        setLocationByPlatform(true);

        getContentPane().setBackground(BACKGROUND);
        resultArea.setEditable(false);
        dataArea.setLineWrap(true);
        dataArea.setWrapStyleWord(true);
        resultArea.setLineWrap(false);
        styleField(hostField);
        styleField(coordinatorIdField);
        styleField(coordinatorPortField);
        styleTextArea(dataArea);
        styleTextArea(resultArea);
        styleComboBox(jobTypeCombo);
        stylePrimaryButton(submitButton);
        styleSecondaryButton(startElectionButton);
        styleSecondaryButton(refreshNodesButton);
        styleSecondaryButton(clearInputButton);
        styleSecondaryButton(exitButton);
        styleStatusLabel(statusLabel);

        submitButton.addActionListener(event -> submitJob());
        startElectionButton.addActionListener(event -> startElection());
        refreshNodesButton.addActionListener(event -> refreshNodeStatus(true));
        clearInputButton.addActionListener(event -> clearInput());
        exitButton.addActionListener(event -> exitApplication());
        coordinatorIdField.addActionListener(event -> updatePortFromCoordinatorId());
        systemRefreshTimer = new Timer(3000, event -> {
            if (refreshNodesButton.isEnabled()) {
                refreshNodeStatus();
            }
        });
        dataArea.getDocument().addDocumentListener(new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent event) {
                updateSubmitButtonState();
            }

            @Override
            public void removeUpdate(DocumentEvent event) {
                updateSubmitButtonState();
            }

            @Override
            public void changedUpdate(DocumentEvent event) {
                updateSubmitButtonState();
            }
        });

        JPanel content = new JPanel(new BorderLayout(8, 8));
        content.setBackground(BACKGROUND);
        content.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
        content.add(headerPanel(), BorderLayout.NORTH);
        content.add(darkPanelScrollPane(workspacePanel()), BorderLayout.CENTER);
        setContentPane(content);
        pack();
        statusLabel.setText("Enter 6-15 numbers, submit job, then start election");
        updateSubmitButtonState();
        refreshNodeStatus();
        systemRefreshTimer.start();
    }

    private JPanel headerPanel() {
        JPanel wrapper = new JPanel(new BorderLayout(0, 8));
        wrapper.setOpaque(false);

        JPanel titlePanel = new JPanel(new BorderLayout(8, 4));
        titlePanel.setOpaque(false);
        JLabel title = new JLabel("DistriLab Distributed Computing Cluster");
        title.setForeground(TEXT);
        title.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 24));
        JLabel subtitle = new JLabel(getTitle() + " - submit jobs to the elected 4-worker RMI cluster");
        subtitle.setForeground(MUTED_TEXT);
        subtitle.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
        titlePanel.add(title, BorderLayout.NORTH);
        titlePanel.add(subtitle, BorderLayout.SOUTH);

        wrapper.add(titlePanel, BorderLayout.NORTH);
        return wrapper;
    }

    private JPanel workspacePanel() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setOpaque(false);

        JPanel system = systemStatusPanel();
        system.setAlignmentX(LEFT_ALIGNMENT);
        JPanel cluster = clusterPanel();
        cluster.setAlignmentX(LEFT_ALIGNMENT);
        JPanel job = jobPanel();
        job.setAlignmentX(LEFT_ALIGNMENT);
        JPanel actions = actionsPanel();
        actions.setAlignmentX(LEFT_ALIGNMENT);

        panel.add(system);
        panel.add(Box.createVerticalStrut(8));
        panel.add(cluster);
        panel.add(Box.createVerticalStrut(8));
        panel.add(job);
        panel.add(Box.createVerticalStrut(8));
        panel.add(actions);
        return panel;
    }

    private void fitSectionWidth(JPanel panel) {
        panel.setMaximumSize(new Dimension(Integer.MAX_VALUE, panel.getPreferredSize().height));
    }

    private JPanel systemStatusPanel() {
        JPanel panel = cardPanel("SYSTEM STATUS");
        panel.setLayout(new BorderLayout(8, 8));

        JPanel metrics = new JPanel(new GridLayout(0, 1, 6, 6));
        metrics.setOpaque(false);
        metrics.add(metricPanel("Active Workers", activeWorkersValue));
        metrics.add(metricPanel("Current Leader", leaderValue));
        metrics.add(metricPanel("Jobs This Term", jobsThisTermValue));

        JPanel controls = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        controls.setOpaque(false);
        controls.add(refreshNodesButton);

        panel.add(metrics, BorderLayout.CENTER);
        panel.add(controls, BorderLayout.SOUTH);
        fitSectionWidth(panel);
        return panel;
    }

    private JPanel metricPanel(String labelText, JLabel valueLabel) {
        JPanel panel = new JPanel(new BorderLayout(0, 5));
        panel.setBackground(PANEL_ALT);
        panel.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(BORDER),
                BorderFactory.createEmptyBorder(8, 12, 8, 12)));
        JLabel label = new JLabel(labelText);
        styleMutedLabel(label);
        valueLabel.setForeground(TEXT);
        valueLabel.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 18));
        panel.add(label, BorderLayout.NORTH);
        panel.add(valueLabel, BorderLayout.CENTER);
        return panel;
    }

    private JPanel clusterPanel() {
        JPanel panel = cardPanel("WORKER RUNTIME STATUS");
        panel.setLayout(new BorderLayout(8, 8));

        JPanel topRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 2));
        topRow.setOpaque(false);
        JLabel note = new JLabel("Workers are started in terminals; refresh to check current process status");
        styleMutedLabel(note);
        topRow.add(note);

        JPanel nodes = new JPanel(new GridLayout(2, 2, 8, 8));
        nodes.setOpaque(false);
        nodes.setPreferredSize(new Dimension(440, 156));
        nodeCards.clear();
        for (int workerId : WorkerClusterConfig.workerIds()) {
            NodeCard nodeCard = createNodeCard(workerId);
            updateNodeCard(nodeCard, new NodeState(workerId, WorkerClusterConfig.portFor(workerId),
                    "Offline", "-", "-", "-", "-", DANGER));
            nodeCards.add(nodeCard);
            nodes.add(nodeCard.panel);
        }

        panel.add(topRow, BorderLayout.NORTH);
        panel.add(nodes, BorderLayout.CENTER);
        fitSectionWidth(panel);
        return panel;
    }

    private JPanel actionsPanel() {
        JPanel panel = new JPanel(new BorderLayout(8, 6));
        panel.setOpaque(false);
        panel.setBorder(BorderFactory.createEmptyBorder(0, 2, 0, 2));

        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 0));
        buttons.setOpaque(false);
        buttons.add(submitButton);
        buttons.add(startElectionButton);
        buttons.add(exitButton);

        panel.add(buttons, BorderLayout.NORTH);
        panel.add(statusLabel, BorderLayout.CENTER);
        fitSectionWidth(panel);
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
        gbc.insets = new Insets(10, 12, 10, 10);
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
        controls.add(clearInputButton);

        panel.add(controls);
        panel.add(Box.createVerticalStrut(8));
        JLabel inputLabel = new JLabel("Input 6 to 15 integers, separated by commas, spaces, or new lines");
        inputLabel.setAlignmentX(LEFT_ALIGNMENT);
        styleMutedLabel(inputLabel);
        panel.add(inputLabel);
        panel.add(Box.createVerticalStrut(6));
        JScrollPane scrollPane = darkScrollPane(dataArea);
        scrollPane.setAlignmentX(LEFT_ALIGNMENT);
        panel.add(scrollPane);
        fitSectionWidth(panel);
        return panel;
    }

    private void submitJob() {
        if (!hasValidInputCount()) {
            JOptionPane.showMessageDialog(this, "Enter between 6 and 15 integers before submitting a job.",
                    "Input Required", JOptionPane.WARNING_MESSAGE);
            updateSubmitButtonState();
            return;
        }
        if (jobsThisTerm >= MAX_JOBS_PER_TERM) {
            JOptionPane.showMessageDialog(this, "Only 5 jobs can be submitted per coordinator term. "
                            + "Start a new election before submitting more jobs.",
                    "Job Limit Reached", JOptionPane.WARNING_MESSAGE);
            updateSubmitButtonState();
            return;
        }

        JobRequest request;
        try {
            request = readJobRequest();
        } catch (IllegalArgumentException e) {
            JOptionPane.showMessageDialog(this, e.getMessage(), "Invalid Job", JOptionPane.WARNING_MESSAGE);
            return;
        }

        pendingJobRequest = request;
        updateSubmitButtonState();
        statusLabel.setText(request.jobType + " job submitted - start election to run it");
        setResultContent(formatPendingJobResult(request));
    }

    private boolean hasValidInputCount() {
        try {
            int count = parseNumbers(dataArea.getText()).size();
            return count >= MIN_INPUT_VALUES && count <= MAX_INPUT_VALUES;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private void updateSubmitButtonState() {
        submitButton.setEnabled(hasValidInputCount() && pendingJobRequest == null
                && !jobInProgress && !electionInProgress && jobsThisTerm < MAX_JOBS_PER_TERM);
        startElectionButton.setEnabled(pendingJobRequest != null && !jobInProgress && !electionInProgress);
    }

    private void clearInput() {
        dataArea.setText("");
        pendingJobRequest = null;
        setResultContent("");
        statusLabel.setText("Input cleared");
        updateSubmitButtonState();
    }

    private void exitApplication() {
        systemRefreshTimer.stop();
        dispose();
        System.exit(0);
    }

    private void startElection() {
        if (pendingJobRequest == null) {
            JOptionPane.showMessageDialog(this, "Submit a job before starting an election.",
                    "Job Required", JOptionPane.WARNING_MESSAGE);
            updateSubmitButtonState();
            return;
        }

        String host = hostField.getText().trim();
        if (host.isEmpty()) {
            host = WorkerClusterConfig.DEFAULT_HOST;
        }

        electionInProgress = true;
        updateSubmitButtonState();
        statusLabel.setText("Starting leader election...");
        appendResult("[" + now() + "] Starting leader election from worker "
                + WorkerClusterConfig.FIRST_WORKER_ID);

        new ElectionWorker(host).execute();
    }

    private void refreshNodeStatus() {
        refreshNodeStatus(false);
    }

    private void refreshNodeStatus(boolean clearCurrentLeader) {
        String host = hostField.getText().trim();
        if (host.isEmpty()) {
            host = WorkerClusterConfig.DEFAULT_HOST;
        }

        refreshNodesButton.setEnabled(false);
        if (clearCurrentLeader) {
            leaderValue.setText("none");
            coordinatorIdField.setText(String.valueOf(WorkerClusterConfig.LAST_WORKER_ID));
            coordinatorPortField.setText(String.valueOf(WorkerClusterConfig.portFor(WorkerClusterConfig.LAST_WORKER_ID)));
            jobsThisTerm = 0;
            updateJobsThisTermLabel();
        }
        statusLabel.setText(clearCurrentLeader ? "Clearing current leader and checking workers..."
                : "Checking worker nodes...");
        if (!nodeCards.isEmpty()) {
            for (int i = 0; i < nodeCards.size(); i++) {
                int workerId = WorkerClusterConfig.FIRST_WORKER_ID + i;
                updateNodeCard(nodeCards.get(i), new NodeState(workerId, WorkerClusterConfig.portFor(workerId),
                        "Checking", "-", "-", "-", "-", MUTED_TEXT));
            }
        }

        Set<Integer> workersToCheck = new HashSet<>();
        for (int workerId : WorkerClusterConfig.workerIds()) {
            workersToCheck.add(workerId);
        }
        new NodeStatusWorker(host, workersToCheck, clearCurrentLeader).execute();
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
            throw new IllegalArgumentException("Enter between 6 and 15 integers.");
        }
        if (numbers.size() < MIN_INPUT_VALUES) {
            throw new IllegalArgumentException("Enter at least " + MIN_INPUT_VALUES + " integers.");
        }
        if (numbers.size() > MAX_INPUT_VALUES) {
            throw new IllegalArgumentException("Enter no more than " + MAX_INPUT_VALUES + " integers.");
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

    private void setResultContent(String text) {
        resultArea.setText(text + System.lineSeparator());
        resultArea.setCaretPosition(0);
    }

    private String formatJobResult(JobRequest request, String result) {
        return "JOB RESULT - " + clientName + System.lineSeparator()
                + System.lineSeparator()
                + String.format("%-16s%s%n", "Job Type:", request.jobType)
                + String.format("%-16s%s%n", "Input:", formatNumbers(request.numbers))
                + String.format("%-16sWorker %d%n", "Coordinator:", request.coordinatorId)
                + String.format("%-16s%d%n", "Workers Used:", currentWorkersUsed())
                + String.format("%-16s%s%n", "Final Result:", result)
                + String.format("%-16s%s", "Status:", "Completed");
    }

    private String formatPendingJobResult(JobRequest request) {
        return "JOB RESULT - " + clientName + System.lineSeparator()
                + System.lineSeparator()
                + String.format("%-16s%s%n", "Job Type:", request.jobType)
                + String.format("%-16s%s%n", "Input:", formatNumbers(request.numbers))
                + String.format("%-16sWorker %d%n", "Coordinator:", request.coordinatorId)
                + String.format("%-16s%d%n", "Workers Used:", currentWorkersUsed())
                + String.format("%-16s%s%n", "Final Result:", "Pending")
                + String.format("%-16s%s", "Status:", "Running");
    }

    private String formatFailedJobResult(JobRequest request, String message) {
        return "JOB RESULT - " + clientName + System.lineSeparator()
                + System.lineSeparator()
                + String.format("%-16s%s%n", "Job Type:", request.jobType)
                + String.format("%-16s%s%n", "Input:", formatNumbers(request.numbers))
                + String.format("%-16sWorker %d%n", "Coordinator:", request.coordinatorId)
                + String.format("%-16s%s%n", "Workers Used:", currentWorkersUsed())
                + String.format("%-16s%s%n", "Final Result:", "-")
                + String.format("%-16s%s%n", "Status:", "Failed")
                + String.format("%-16s%s", "Reason:", message);
    }

    private void showJobResultDialog(String report) {
        JTextArea reportView = new JTextArea(report, 9, 48);
        reportView.setEditable(false);
        reportView.setLineWrap(false);
        reportView.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
        reportView.setBackground(PANEL);
        reportView.setForeground(TEXT);
        reportView.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

        JScrollPane scrollPane = new JScrollPane(reportView);
        scrollPane.setBorder(BorderFactory.createLineBorder(BORDER));
        JOptionPane.showMessageDialog(this, scrollPane, "Job Result", JOptionPane.INFORMATION_MESSAGE);
    }

    private String formatNumbers(List<Integer> numbers) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numbers.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(numbers.get(i));
        }
        return builder.toString();
    }

    private int currentWorkersUsed() {
        try {
            return Math.max(1, Integer.parseInt(activeWorkersValue.getText().trim()));
        } catch (NumberFormatException e) {
            return Math.max(1, startedWorkerIds.size());
        }
    }

    private String now() {
        return LocalTime.now().format(TIME_FORMAT);
    }

    private NodeCard createNodeCard(int workerId) {
        JPanel panel = new JPanel(new BorderLayout(0, 8));
        panel.setOpaque(true);

        JLabel details = new JLabel();
        details.setVerticalAlignment(JLabel.TOP);

        JButton startButton = new JButton("Start");
        styleSecondaryButton(startButton);

        panel.add(details, BorderLayout.CENTER);
        return new NodeCard(panel, details, startButton);
    }

    private void updateNodeCard(NodeCard card, NodeState state) {
        boolean leader = "LEADER".equals(state.role);
        card.panel.setBackground(leader ? new Color(255, 251, 235) : PANEL);
        card.details.setForeground(TEXT);
        card.details.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 13));
        Color borderColor = leader ? LEADER : BORDER;
        String statusDot = "&#9679;";
        Color roleColor = leader ? LEADER : TEXT;
        card.panel.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(borderColor),
                BorderFactory.createEmptyBorder(10, 12, 10, 12)));
        String displayStatus = "Online".equals(state.status) ? "Running"
                : "Checking".equals(state.status) ? "Checking" : "Not Running";
        card.details.setText("<html><body style='width: 260px; line-height: 1.2'>"
                + "<b style='font-size: 15px'>Worker " + state.workerId + "</b><br>"
                + "<span style='color: " + hex(MUTED_TEXT) + "'>ID: " + state.workerId + "</span><br>"
                + "<span style='color: " + hex(state.statusColor) + "; font-size: 13px'>"
                + statusDot + " " + displayStatus + "</span><br>"
                + "<span style='color: " + hex(roleColor) + "'>Role: <b>" + state.role + "</b></span>"
                + "</body></html>");
    }

    private void startWorker(int workerId) {
        String host = hostField.getText().trim();
        if (host.isEmpty()) {
            host = WorkerClusterConfig.DEFAULT_HOST;
        }

        for (NodeCard card : nodeCards) {
            card.startButton.setEnabled(false);
        }
        updateNodeCard(nodeCards.get(workerId - WorkerClusterConfig.FIRST_WORKER_ID),
                new NodeState(workerId, WorkerClusterConfig.portFor(workerId),
                        "Starting", "-", "-", "-", "-", ACCENT));
        statusLabel.setText("Starting worker " + workerId + "...");
        appendResult("[" + now() + "] Starting worker " + workerId);
        new WorkerStartWorker(workerId, host).execute();
    }

    private void updateJobsThisTermLabel() {
        jobsThisTermValue.setText(jobsThisTerm + " / " + MAX_JOBS_PER_TERM);
    }

    private String formatCoordinator(int coordinatorId) {
        return coordinatorId == WorkerService.NO_COORDINATOR ? "none" : String.valueOf(coordinatorId);
    }

    private String hex(Color color) {
        return String.format("#%02x%02x%02x", color.getRed(), color.getGreen(), color.getBlue());
    }

    private static void setModernLookAndFeel() {
        try {
            FlatLightLaf.setup();
        } catch (Exception ignored) {
            // Swing will fall back to the default look and feel.
        }
        UIManager.put("OptionPane.background", PANEL);
        UIManager.put("Panel.background", PANEL);
        UIManager.put("OptionPane.messageForeground", TEXT);
        UIManager.put("Component.arc", 10);
        UIManager.put("Button.arc", 10);
        UIManager.put("TextComponent.arc", 10);
        UIManager.put("Component.focusColor", ACCENT);
        UIManager.put("Button.focusedBorderColor", ACCENT);
        UIManager.put("Component.focusedBorderColor", ACCENT);
        UIManager.put("ScrollBar.width", 12);
        UIManager.put("ScrollBar.thumbArc", 999);
        UIManager.put("ScrollBar.trackArc", 999);
    }

    private JPanel cardPanel(String title) {
        JPanel panel = new JPanel();
        panel.setBackground(PANEL);
        TitledBorder titledBorder = BorderFactory.createTitledBorder(
                BorderFactory.createLineBorder(BORDER), title);
        titledBorder.setTitleColor(TEXT);
        titledBorder.setTitleFont(new Font(Font.SANS_SERIF, Font.BOLD, 14));
        panel.setBorder(BorderFactory.createCompoundBorder(
                titledBorder,
                BorderFactory.createEmptyBorder(8, 10, 10, 10)));
        return panel;
    }

    private GridBagConstraints baseConstraints() {
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(2, 0, 2, 8);
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
        scrollPane.putClientProperty("JComponent.roundRect", true);
        return scrollPane;
    }

    private JScrollPane darkPanelScrollPane(JPanel panel) {
        JScrollPane scrollPane = new JScrollPane(panel);
        scrollPane.setBorder(BorderFactory.createEmptyBorder());
        scrollPane.getViewport().setBackground(BACKGROUND);
        scrollPane.setBackground(BACKGROUND);
        scrollPane.getVerticalScrollBar().setUnitIncrement(16);
        scrollPane.putClientProperty("JComponent.roundRect", true);
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
        field.setBorder(BorderFactory.createEmptyBorder(6, 8, 6, 8));
        field.putClientProperty("JComponent.roundRect", true);
    }

    private void styleTextArea(JTextArea textArea) {
        textArea.setBackground(FIELD);
        textArea.setForeground(TEXT);
        textArea.setCaretColor(ACCENT);
        textArea.setSelectionColor(ACCENT_DARK);
        textArea.setSelectedTextColor(Color.WHITE);
        textArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
        textArea.setBorder(BorderFactory.createEmptyBorder(8, 10, 8, 10));
        textArea.putClientProperty("JComponent.roundRect", true);
    }

    private void styleComboBox(JComboBox<JobType> comboBox) {
        comboBox.setBackground(PANEL_ALT);
        comboBox.setForeground(TEXT);
        comboBox.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 13));
        comboBox.putClientProperty("JComponent.roundRect", true);
    }

    private void stylePrimaryButton(JButton button) {
        button.setBackground(ACCENT_DARK);
        button.setForeground(Color.WHITE);
        button.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 13));
        button.setFocusPainted(false);
        button.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        button.setBorder(BorderFactory.createEmptyBorder(7, 14, 7, 14));
        button.putClientProperty("JButton.buttonType", "roundRect");
    }

    private void styleSecondaryButton(JButton button) {
        button.setBackground(PANEL_ALT);
        button.setForeground(TEXT);
        button.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 12));
        button.setFocusPainted(false);
        button.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        button.setBorder(BorderFactory.createEmptyBorder(6, 12, 6, 12));
        button.putClientProperty("JButton.buttonType", "roundRect");
    }

    private enum JobType {
        MAX,
        PRIMESUM,
        PRIMECOUNT
    }

    private record JobRequest(String host, int port, int coordinatorId, JobType jobType, List<Integer> numbers) {
    }

    private record ElectionResult(String message, int coordinatorId) {
    }

    private record NodeCard(JPanel panel, JLabel details, JButton startButton) {
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
                case MAX -> String.valueOf(coordinator.submitMaxJob(clientName, request.numbers));
                case PRIMESUM -> String.valueOf(coordinator.submitPrimeSumJob(clientName, request.numbers));
                case PRIMECOUNT -> String.valueOf(coordinator.submitPrimeCountJob(clientName, request.numbers));
            };
        }

        @Override
        protected void done() {
            try {
                String result = get();
                jobsThisTerm++;
                updateJobsThisTermLabel();
                String report = formatJobResult(request, result);
                setResultContent(report);
                showJobResultDialog(report);
                if (jobsThisTerm >= MAX_JOBS_PER_TERM) {
                    appendResult("[" + now() + "] Job limit reached for this term");
                    updateJobsThisTermLabel();
                    statusLabel.setText("5 job limit reached - start election for a new term");
                } else {
                    statusLabel.setText("Completed " + request.jobType);
                }
            } catch (Exception e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                setResultContent(formatFailedJobResult(request, cause.getMessage()));
                statusLabel.setText("Job failed");
                JOptionPane.showMessageDialog(DistriLabClientGui.this,
                        cause.getMessage(), "Job Failed", JOptionPane.ERROR_MESSAGE);
            } finally {
                jobInProgress = false;
                updateSubmitButtonState();
                refreshNodeStatus();
            }
        }
    }

    private final class ElectionWorker extends SwingWorker<ElectionResult, Void> {
        private final String host;

        private ElectionWorker(String host) {
            this.host = host;
        }

        @Override
        protected ElectionResult doInBackground() throws Exception {
            for (int workerId : WorkerClusterConfig.workerIds()) {
                Registry registry = LocateRegistry.getRegistry(host, WorkerClusterConfig.portFor(workerId));
                WorkerService worker = (WorkerService) registry.lookup(
                        WorkerServer.SERVICE_NAME_PREFIX + workerId);
                worker.setCurrentCoordinatorId(clientName, WorkerService.NO_COORDINATOR);
                worker.syncNeighbours();
            }

            Registry registry = LocateRegistry.getRegistry(host,
                    WorkerClusterConfig.portFor(WorkerClusterConfig.FIRST_WORKER_ID));
            WorkerService initiator = (WorkerService) registry.lookup(
                    WorkerServer.SERVICE_NAME_PREFIX + WorkerClusterConfig.FIRST_WORKER_ID);
            String message = initiator.initiateElection(clientName);
            return new ElectionResult(message, initiator.getCurrentCoordinatorId(clientName));
        }

        @Override
        protected void done() {
            try {
                ElectionResult result = get();
                jobsThisTerm = 0;
                updateJobsThisTermLabel();
                appendResult("[" + now() + "] " + result.message());
                coordinatorIdField.setText(String.valueOf(result.coordinatorId()));
                coordinatorPortField.setText(String.valueOf(WorkerClusterConfig.portFor(result.coordinatorId())));
                leaderValue.setText("Worker " + result.coordinatorId());
                statusLabel.setText("Coordinator elected - running submitted job");
                JOptionPane.showMessageDialog(DistriLabClientGui.this,
                        clientName + System.lineSeparator() + result.message(),
                        clientName + " Coordinator Elected", JOptionPane.INFORMATION_MESSAGE);
                JobRequest request = pendingJobRequest;
                pendingJobRequest = null;
                if (request != null) {
                    JobRequest electedRequest = new JobRequest(request.host(),
                            WorkerClusterConfig.portFor(result.coordinatorId()), result.coordinatorId(),
                            request.jobType(), request.numbers());
                    jobInProgress = true;
                    setResultContent(formatPendingJobResult(electedRequest));
                    new JobWorker(electedRequest).execute();
                }
            } catch (Exception e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                appendResult("[" + now() + "] Election failed: " + cause.getMessage());
                statusLabel.setText("Election failed");
                pendingJobRequest = null;
                JOptionPane.showMessageDialog(DistriLabClientGui.this,
                        cause.getMessage(), "Election Failed", JOptionPane.ERROR_MESSAGE);
            } finally {
                electionInProgress = false;
                updateSubmitButtonState();
                refreshNodeStatus();
            }
        }
    }

    private final class WorkerStartWorker extends SwingWorker<String, Void> {
        private final int workerId;
        private final String host;

        private WorkerStartWorker(int workerId, String host) {
            this.workerId = workerId;
            this.host = host;
        }

        @Override
        protected String doInBackground() throws Exception {
            int port = WorkerClusterConfig.portFor(workerId);
            if (isWorkerOnline(host, workerId, port)) {
                resetWorkerCoordinator(host, workerId, port);
                resetRunningWorkerCoordinators(host);
                return "Worker " + workerId + " is already online";
            }

            Path logDir = Paths.get(WorkerClusterConfig.LOG_DIR);
            Files.createDirectories(logDir);

            String javaBin = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
            String classpath = currentClasspath();
            ProcessBuilder builder = new ProcessBuilder(
                    javaBin,
                    "-cp", classpath,
                    WorkerServer.class.getName(),
                    String.valueOf(workerId),
                    String.valueOf(port),
                    WorkerClusterConfig.DEFAULT_HOST,
                    "1099",
                    WorkerClusterConfig.DEFAULT_HOST);
            builder.directory(Paths.get("").toAbsolutePath().toFile());
            builder.redirectOutput(ProcessBuilder.Redirect.to(logDir.resolve(logFileName(workerId)).toFile()));
            builder.redirectError(ProcessBuilder.Redirect.appendTo(logDir.resolve(logFileName(workerId)).toFile()));

            Process process = builder.start();
            Files.writeString(logDir.resolve(pidFileName(workerId)),
                    String.valueOf(process.pid()), StandardCharsets.UTF_8);
            waitForWorker(host, workerId, port);
            resetWorkerCoordinator(host, workerId, port);
            resetRunningWorkerCoordinators(host);
            return "Worker " + workerId + " started on port " + port;
        }

        @Override
        protected void done() {
            try {
                String result = get();
                startedWorkerIds.add(workerId);
                appendResult("[" + now() + "] " + result);
                statusLabel.setText(result);
            } catch (Exception e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                appendResult("[" + now() + "] Worker " + workerId + " start failed: " + cause.getMessage());
                statusLabel.setText("Worker " + workerId + " start failed");
                JOptionPane.showMessageDialog(DistriLabClientGui.this,
                        cause.getMessage(), "Worker Start Failed", JOptionPane.ERROR_MESSAGE);
            } finally {
                refreshNodeStatus();
            }
        }

        private boolean isWorkerOnline(String lookupHost, int lookupWorkerId, int port) {
            try {
                Registry registry = LocateRegistry.getRegistry(lookupHost, port);
                WorkerService worker = (WorkerService) registry.lookup(
                        WorkerServer.SERVICE_NAME_PREFIX + lookupWorkerId);
                return worker.getWorkerId() == lookupWorkerId;
            } catch (Exception e) {
                return false;
            }
        }

        private void resetWorkerCoordinator(String lookupHost, int lookupWorkerId, int port) throws Exception {
            Registry registry = LocateRegistry.getRegistry(lookupHost, port);
            WorkerService worker = (WorkerService) registry.lookup(
                    WorkerServer.SERVICE_NAME_PREFIX + lookupWorkerId);
            worker.setCurrentCoordinatorId(clientName, WorkerService.NO_COORDINATOR);
            worker.syncNeighbours();
        }

        private void resetRunningWorkerCoordinators(String lookupHost) {
            for (int id : WorkerClusterConfig.workerIds()) {
                int port = WorkerClusterConfig.portFor(id);
                try {
                    if (isWorkerOnline(lookupHost, id, port)) {
                        resetWorkerCoordinator(lookupHost, id, port);
                    }
                } catch (Exception e) {
                    System.err.println("Could not clear coordinator for worker " + id + ": " + e.getMessage());
                }
            }
        }

        private void waitForWorker(String lookupHost, int lookupWorkerId, int port) throws Exception {
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                if (isWorkerOnline(lookupHost, lookupWorkerId, port)) {
                    return;
                }
                Thread.sleep(400);
            }
            throw new IOException("Worker " + lookupWorkerId + " did not answer on port " + port);
        }

        private String currentClasspath() {
            try {
                return Paths.get(DistriLabClientGui.class.getProtectionDomain()
                        .getCodeSource().getLocation().toURI()).toAbsolutePath().toString();
            } catch (Exception e) {
                return System.getProperty("java.class.path");
            }
        }

        private String logFileName(int workerId) {
            return "worker-" + workerId + ".log";
        }

        private String pidFileName(int workerId) {
            return "worker-" + workerId + ".pid";
        }
    }

    private final class NodeStatusWorker extends SwingWorker<List<NodeState>, Void> {
        private final String host;
        private final Set<Integer> workersToCheck;
        private final boolean clearCurrentLeader;

        private NodeStatusWorker(String host, Set<Integer> workersToCheck, boolean clearCurrentLeader) {
            this.host = host;
            this.workersToCheck = workersToCheck;
            this.clearCurrentLeader = clearCurrentLeader;
        }

        @Override
        protected List<NodeState> doInBackground() {
            List<NodeState> states = new ArrayList<>();
            for (int workerId : WorkerClusterConfig.workerIds()) {
                int port = WorkerClusterConfig.portFor(workerId);
                if (!workersToCheck.contains(workerId)) {
                    states.add(new NodeState(workerId, port, "Offline", "-", "-", "-", "-", DANGER));
                    continue;
                }
                try {
                    Registry registry = LocateRegistry.getRegistry(host, port);
                    WorkerService worker = (WorkerService) registry.lookup(
                            WorkerServer.SERVICE_NAME_PREFIX + workerId);
                    if (clearCurrentLeader) {
                        worker.setCurrentCoordinatorId(clientName, WorkerService.NO_COORDINATOR);
                    }
                    worker.syncNeighbours();
                    int coordinator = worker.getCurrentCoordinatorId(clientName);
                    int jac = worker.getJobAllocationCounter();
                    String role = coordinator == workerId ? "LEADER" : "Worker";
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
                String leader = "none";
                for (int i = 0; i < states.size(); i++) {
                    NodeState state = states.get(i);
                    if ("Online".equals(state.status)) {
                        online++;
                    }
                    if ("LEADER".equals(state.role)) {
                        leader = "Worker " + state.workerId;
                        coordinatorIdField.setText(String.valueOf(state.workerId));
                        coordinatorPortField.setText(String.valueOf(state.port));
                    }
                    if (i < nodeCards.size()) {
                        updateNodeCard(nodeCards.get(i), state);
                    }
                }
                activeWorkersValue.setText(String.valueOf(online));
                leaderValue.setText(clearCurrentLeader ? "none" : leader);
                if (pendingJobRequest == null && !jobInProgress && !electionInProgress) {
                    statusLabel.setText(clearCurrentLeader
                            ? "Current leader cleared; " + online + "/" + WorkerClusterConfig.WORKER_COUNT
                                    + " worker nodes online"
                            : online + "/" + WorkerClusterConfig.WORKER_COUNT + " worker nodes online");
                }
                updateSubmitButtonState();
            } catch (Exception e) {
                statusLabel.setText("Could not refresh worker nodes");
            } finally {
                refreshNodesButton.setEnabled(true);
            }
        }
    }
}
