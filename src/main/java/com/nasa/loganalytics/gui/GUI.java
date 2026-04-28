package com.nasa.loganalytics.gui;

import com.nasa.loganalytics.db.*;
import com.nasa.loganalytics.model.LogRecord;
import com.nasa.loganalytics.parser.LogParser;
import com.nasa.loganalytics.store.MySQLStore;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;

public class GUI extends JFrame {

    private JComboBox<String> pipelineCombo;
    private JTextField batchSizeField;
    private JTextArea outputArea;
    private JButton loadBtn, q1Btn, q2Btn, q3Btn, runAllBtn, reportBtn, viewAllBtn;

    // Config
    private String mysqlHost = "localhost";
    private int mysqlPort = 3306;
    private String mysqlUser = "root";
    private String mysqlPassword = "kunaljindal19OSR";
    private String mongoHost = "localhost";
    private int mongoPort = 27017;
    private String dataDir = "Dataset";

    // State
    private Database currentDb = null;
    private MySQLStore mysqlStore = null;
    private boolean dataLoaded = false;
    private String currentRunId = null;
    private int totalRecords = 0;
    private int numBatches = 0;
    private int malformedCount = 0;

    public GUI(String[] args) {
        super("NASA Log Analytics Tool");
        parseArgs(args);
        initUI();
        initStore();
        
        setSize(1000, 700);
        setLocationRelativeTo(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--mysql-host": mysqlHost = args[++i]; break;
                case "--mysql-port": mysqlPort = Integer.parseInt(args[++i]); break;
                case "--mysql-user": mysqlUser = args[++i]; break;
                case "--mysql-pass": mysqlPassword = args[++i]; break;
                case "--mongo-host": mongoHost = args[++i]; break;
                case "--mongo-port": mongoPort = Integer.parseInt(args[++i]); break;
                case "--data-dir":   dataDir = args[++i]; break;
            }
        }
    }

    private void initUI() {
        setLayout(new BorderLayout(10, 10));
        ((JPanel)getContentPane()).setBorder(new EmptyBorder(10, 10, 10, 10));

        // --- North Panel: Configuration & Loading ---
        JPanel topPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 15, 5));
        
        topPanel.add(new JLabel("Pipeline:"));
        pipelineCombo = new JComboBox<>(new String[]{"MongoDB", "MapReduce", "ApachePig", "ApacheHive"});
        topPanel.add(pipelineCombo);

        topPanel.add(new JLabel("Batch Size:"));
        batchSizeField = new JTextField("50000", 6);
        topPanel.add(batchSizeField);

        loadBtn = new JButton("Load Dataset");
        loadBtn.setBackground(new Color(46, 134, 193));
        loadBtn.setForeground(Color.WHITE);
        loadBtn.setOpaque(true);
        loadBtn.setBorderPainted(false);
        topPanel.add(loadBtn);

        add(topPanel, BorderLayout.NORTH);

        // --- West Panel: Queries ---
        q1Btn = new JButton("Run Query 1");
        q2Btn = new JButton("Run Query 2");
        q3Btn = new JButton("Run Query 3");
        runAllBtn = new JButton("Run All Queries");
        reportBtn = new JButton("View Run History");
        viewAllBtn = new JButton("View Full Report");
        
        reportBtn.setBackground(new Color(39, 174, 96));
        reportBtn.setForeground(Color.WHITE);
        reportBtn.setOpaque(true);
        reportBtn.setBorderPainted(false);
        
        viewAllBtn.setBackground(new Color(142, 68, 173));
        viewAllBtn.setForeground(Color.WHITE);
        viewAllBtn.setOpaque(true);
        viewAllBtn.setBorderPainted(false);

        JPanel leftPanel = new JPanel(new GridLayout(7, 1, 0, 10));
        leftPanel.setBorder(new EmptyBorder(10, 5, 10, 15));

        leftPanel.add(new JLabel("<html><b>Execute Queries</b></html>"));
        leftPanel.add(q1Btn);
        leftPanel.add(q2Btn);
        leftPanel.add(q3Btn);
        leftPanel.add(runAllBtn);
        leftPanel.add(reportBtn);
        leftPanel.add(viewAllBtn);

        add(leftPanel, BorderLayout.WEST);

        // --- Center Panel: Output ---
        outputArea = new JTextArea();
        outputArea.setEditable(false);
        outputArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        outputArea.setBackground(new Color(40, 44, 52));
        outputArea.setForeground(new Color(171, 178, 191));
        
        JScrollPane scrollPane = new JScrollPane(outputArea);
        add(scrollPane, BorderLayout.CENTER);

        // --- Event Listeners ---
        loadBtn.addActionListener(e -> executeLoad());
        q1Btn.addActionListener(e -> executeQuery("query1", "Daily Traffic Summary"));
        q2Btn.addActionListener(e -> executeQuery("query2", "Top 20 Requested Resources"));
        q3Btn.addActionListener(e -> executeQuery("query3", "Hourly Error Analysis"));
        runAllBtn.addActionListener(e -> executeRunAll());
        reportBtn.addActionListener(e -> executeReport());
        viewAllBtn.addActionListener(e -> executeViewAll());
    }

    private void initStore() {
        try {
            mysqlStore = new MySQLStore(mysqlHost, mysqlPort, mysqlUser, mysqlPassword);
            mysqlStore.initialize();
            appendLog("✓ Connected to MySQL at " + mysqlHost + ":" + mysqlPort);
        } catch (Exception e) {
            appendLog("✗ Failed to connect to MySQL: " + e.getMessage());
            appendLog("  Results will not be stored.");
        }
    }

    private void appendLog(String msg) {
        SwingUtilities.invokeLater(() -> {
            outputArea.append(msg + "\n");
            outputArea.setCaretPosition(outputArea.getDocument().getLength());
        });
    }

    private void setButtonsEnabled(boolean enabled) {
        loadBtn.setEnabled(enabled);
        q1Btn.setEnabled(enabled);
        q2Btn.setEnabled(enabled);
        q3Btn.setEnabled(enabled);
        runAllBtn.setEnabled(enabled);
        reportBtn.setEnabled(enabled);
        viewAllBtn.setEnabled(enabled);
        pipelineCombo.setEnabled(enabled);
    }

    private void setupPipeline() throws Exception {
        String selected = (String) pipelineCombo.getSelectedItem();
        if (currentDb != null) {
            try { currentDb.cleanup(); } catch (Exception ignored) {}
        }
        
        if ("MongoDB".equals(selected)) {
            currentDb = new MongoDBDatabase(mongoHost, mongoPort);
        } else if ("MapReduce".equals(selected)) {
            currentDb = new MapReduceDatabase();
        } else if ("ApachePig".equals(selected)) {
            currentDb = new ApachePigDatabase();
        } else if ("ApacheHive".equals(selected)) {
            currentDb = new ApacheHiveDatabase();
        }
        currentDb.initialize();
        appendLog("✓ Switched to " + currentDb.getPipelineName() + " pipeline");
    }

    private void executeLoad() {
        setButtonsEnabled(false);
        outputArea.setText("");
        appendLog("Initializing Load process...");
        
        int batchSize;
        try {
            batchSize = Integer.parseInt(batchSizeField.getText().trim());
        } catch (Exception e) {
            appendLog("✗ Invalid batch size.");
            setButtonsEnabled(true);
            return;
        }

        SwingWorker<Void, String> worker = new SwingWorker<>() {
            @Override
            protected Void doInBackground() throws Exception {
                setupPipeline();
                
                File dataDirectory = new File(dataDir);
                if (!dataDirectory.exists() || !dataDirectory.isDirectory()) {
                    publish("✗ Dataset directory not found: " + dataDir);
                    return null;
                }

                File[] dataFiles = dataDirectory.listFiles((dir, name) ->
                        name.endsWith(".gz") || name.endsWith(".log") || name.endsWith(".txt"));
                if (dataFiles == null || dataFiles.length == 0) {
                    publish("✗ No log files found in " + dataDir);
                    return null;
                }
                
                List<File> files = new ArrayList<>(Arrays.asList(dataFiles));
                Collections.sort(files);

                publish("ℹ Loading " + files.size() + " file(s) with batch size " + batchSize + "...");

                currentRunId = currentDb.getPipelineName().toLowerCase() + "-" +
                        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));

                LogParser parser = new LogParser();
                long startTime = System.currentTimeMillis();

                List<List<LogRecord>> batches = parser.readAndBatch(files, batchSize);
                totalRecords = 0;
                numBatches = batches.size();

                for (int i = 0; i < batches.size(); i++) {
                    List<LogRecord> batch = batches.get(i);
                    int batchId = i + 1;
                    currentDb.loadData(batch, batchId);
                    totalRecords += batch.size();
                    
                    if (batchId % 20 == 0 || batchId == batches.size()) {
                        publish("  Batch " + batchId + "/" + batches.size() + " (" + totalRecords + " records)");
                    }
                }

                long elapsed = System.currentTimeMillis() - startTime;
                malformedCount = parser.getMalformedCount();

                currentDb.setBatchSize(batchSize);
                currentDb.setRunId(currentRunId);
                currentDb.setTotalRecords(totalRecords);
                currentDb.setNumBatches(numBatches);
                currentDb.setMalformedCount(malformedCount);
                dataLoaded = true;

                publish(String.format("✓ Loaded %,d records in %d batches (%.1fs)", totalRecords, numBatches, elapsed / 1000.0));
                return null;
            }

            @Override
            protected void process(List<String> chunks) {
                for (String msg : chunks) appendLog(msg);
            }

            @Override
            protected void done() {
                try { get(); } catch (Exception e) { appendLog("✗ Error: " + e.getMessage()); }
                setButtonsEnabled(true);
            }
        };
        worker.execute();
    }

    private void executeQuery(String queryName, String queryLabel) {
        if (!dataLoaded || currentDb == null) {
            appendLog("✗ No data loaded. Please load data first.");
            return;
        }

        setButtonsEnabled(false);
        appendLog("\n════════════════════════════════════════════════════");
        appendLog("Running " + queryLabel + " on " + currentDb.getPipelineName() + "...");

        SwingWorker<Void, String> worker = new SwingWorker<>() {
            @Override
            protected Void doInBackground() throws Exception {
                long startTime = System.currentTimeMillis();
                List<Map<String, Object>> results = null;

                switch (queryName) {
                    case "query1": results = currentDb.executeDailyTrafficSummary(); break;
                    case "query2": results = currentDb.executeTopRequestedResources(); break;
                    case "query3": results = currentDb.executeHourlyErrorAnalysis(); break;
                }

                double execTime = (System.currentTimeMillis() - startTime) / 1000.0;
                publish(String.format("✓ Execution completed in %.3fs", execTime));

                formatAndPublishResults(results, queryName);

                if (mysqlStore != null && results != null) {
                    try {
                        mysqlStore.storeResults(queryName, currentDb.getPipelineName(), currentRunId,
                                0, execTime, currentDb.getBatchSize(), totalRecords, numBatches,
                                currentDb.getAvgBatchSize(), malformedCount, results);
                        publish("✓ Results stored in MySQL.");
                    } catch (Exception e) {
                        publish("✗ MySQL Store Error: " + e.getMessage());
                    }
                }
                return null;
            }

            @Override
            protected void process(List<String> chunks) {
                for (String msg : chunks) appendLog(msg);
            }

            @Override
            protected void done() {
                try { get(); } catch (Exception e) { appendLog("✗ Error: " + e.getMessage()); }
                setButtonsEnabled(true);
            }
        };
        worker.execute();
    }

    private void executeRunAll() {
        if (!dataLoaded || currentDb == null) {
            appendLog("✗ No data loaded. Please load data first.");
            return;
        }
        
        setButtonsEnabled(false);
        SwingWorker<Void, String> worker = new SwingWorker<>() {
            @Override
            protected Void doInBackground() throws Exception {
                publish("\n--- Running All Queries ---");
                
                String[] queries = {"query1", "query2", "query3"};
                for (String q : queries) {
                    long startTime = System.currentTimeMillis();
                    List<Map<String, Object>> results = null;
                    if (q.equals("query1")) results = currentDb.executeDailyTrafficSummary();
                    if (q.equals("query2")) results = currentDb.executeTopRequestedResources();
                    if (q.equals("query3")) results = currentDb.executeHourlyErrorAnalysis();
                    
                    double execTime = (System.currentTimeMillis() - startTime) / 1000.0;
                    publish("\n" + q.toUpperCase() + " completed in " + execTime + "s");
                    
                    formatAndPublishResults(results, q);
                    
                    if (mysqlStore != null && results != null) {
                        mysqlStore.storeResults(q, currentDb.getPipelineName(), currentRunId,
                                0, execTime, currentDb.getBatchSize(), totalRecords, numBatches,
                                currentDb.getAvgBatchSize(), malformedCount, results);
                    }
                }
                publish("\n✓ All queries executed and stored.");
                return null;
            }

            @Override
            protected void process(List<String> chunks) {
                for (String msg : chunks) appendLog(msg);
            }

            @Override
            protected void done() {
                try { get(); } catch (Exception e) { appendLog("✗ Error: " + e.getMessage()); }
                setButtonsEnabled(true);
            }
        };
        worker.execute();
    }

    private void executeReport() {
        if (mysqlStore == null) {
            appendLog("✗ MySQL not connected. Cannot retrieve reports.");
            return;
        }
        
        setButtonsEnabled(false);
        SwingWorker<Void, String> worker = new SwingWorker<>() {
            @Override
            protected Void doInBackground() throws Exception {
                publish("\n════════════════════════════════════════════════════");
                publish("  Stored Query Results (Recent 10 Runs)");
                publish("════════════════════════════════════════════════════");
                
                List<Map<String, Object>> records = mysqlStore.getRecentResults(10);
                if (records.isEmpty()) {
                    publish("No results found in MySQL.");
                    return null;
                }
                
                for (Map<String, Object> record : records) {
                    publish(String.format("Pipeline: %-15s Query: %s", record.get("pipeline_name"), record.get("query_name")));
                    publish(String.format("Run ID:   %-15s Exec Time: %.3fs", record.get("run_id"), record.get("execution_time")));
                    publish(String.format("Records:  %-15s Date: %s", record.get("total_records"), record.get("executed_at")));
                    publish("----------------------------------------------------");
                }
                return null;
            }

            @Override
            protected void process(List<String> chunks) {
                for (String msg : chunks) appendLog(msg);
            }

            @Override
            protected void done() {
                try { get(); } catch (Exception e) { appendLog("✗ Error: " + e.getMessage()); }
                setButtonsEnabled(true);
            }
        };
        worker.execute();
    }

    private void executeViewAll() {
        if (mysqlStore == null) {
            appendLog("✗ MySQL not connected. Cannot generate report.");
            return;
        }
        
        setButtonsEnabled(false);
        outputArea.setText(""); // clear previous
        
        SwingWorker<Void, String> worker = new SwingWorker<>() {
            @Override
            protected Void doInBackground() throws Exception {
                publish("ℹ Exporting results to query_results_output.txt...");
                mysqlStore.exportAllResultsToFile("query_results_output.txt");
                publish("✓ Export complete. Reading file contents...\n");
                publish("═════════════════════════════════════════════════════════════════\n");
                
                File file = new File("query_results_output.txt");
                if (!file.exists()) {
                    publish("✗ File not found.");
                    return null;
                }
                
                try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        publish(line);
                    }
                }
                return null;
            }

            @Override
            protected void process(List<String> chunks) {
                for (String msg : chunks) appendLog(msg);
            }

            @Override
            protected void done() {
                try { get(); } catch (Exception e) { appendLog("✗ Error: " + e.getMessage()); }
                setButtonsEnabled(true);
            }
        };
        worker.execute();
    }

    private void formatAndPublishResults(List<Map<String, Object>> results, String queryName) {
        if (results == null || results.isEmpty()) {
            appendLog("  No results returned.");
            return;
        }

        List<String> columns = new ArrayList<>(results.get(0).keySet());
        int[] widths = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) widths[i] = columns.get(i).length();
        
        for (Map<String, Object> row : results) {
            int i = 0;
            for (String col : columns) {
                String val = formatValue(row.get(col));
                widths[i] = Math.max(widths[i], val.length());
                i++;
            }
        }

        StringBuilder header = new StringBuilder();
        StringBuilder sep = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            header.append(String.format("%-" + widths[i] + "s", columns.get(i)));
            sep.append("-".repeat(widths[i]));
            if (i < columns.size() - 1) {
                header.append(" | ");
                sep.append("-+-");
            }
        }
        appendLog(header.toString());
        appendLog(sep.toString());

        int limit = queryName.equals("query2") ? 20 : 30;
        int shown = 0;
        for (Map<String, Object> row : results) {
            if (shown >= limit) break;
            StringBuilder line = new StringBuilder();
            int i = 0;
            for (String col : columns) {
                line.append(String.format("%-" + widths[i] + "s", formatValue(row.get(col))));
                if (i < columns.size() - 1) line.append(" | ");
                i++;
            }
            appendLog(line.toString());
            shown++;
        }
        if (results.size() > limit) {
            appendLog("... and " + (results.size() - limit) + " more rows");
        }
    }

    private String formatValue(Object val) {
        if (val == null) return "null";
        if (val instanceof Double) return String.format("%.6f", (Double) val);
        if (val instanceof Long) return String.format("%,d", (Long) val);
        if (val instanceof Integer) return String.format("%,d", (Integer) val);
        return val.toString();
    }

    public static void main(String[] args) {
        // Set cross-platform Look & Feel for consistent UI
        try {
            UIManager.setLookAndFeel(UIManager.getCrossPlatformLookAndFeelClassName());
        } catch (Exception ignored) {}

        SwingUtilities.invokeLater(() -> {
            new GUI(args).setVisible(true);
        });
    }
}
