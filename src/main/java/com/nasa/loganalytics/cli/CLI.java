package com.nasa.loganalytics.cli;

import com.nasa.loganalytics.db.*;
import com.nasa.loganalytics.model.LogRecord;
import com.nasa.loganalytics.parser.LogParser;
import com.nasa.loganalytics.store.MySQLStore;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Interactive CLI for the NASA Log Analytics Tool.
 * 
 * Provides a REPL interface for selecting pipelines, loading data,
 * running queries, and viewing reports from MySQL.
 */
public class CLI {

    // ANSI colors for terminal output
    private static final String RESET   = "\033[0m";
    private static final String BOLD    = "\033[1m";
    private static final String GREEN   = "\033[32m";
    private static final String YELLOW  = "\033[33m";
    private static final String RED     = "\033[31m";
    private static final String CYAN    = "\033[36m";
    private static final String BLUE    = "\033[34m";
    private static final String DIM     = "\033[2m";

    // Configuration
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
    private int batchSize = 10000;
    private String currentRunId = null;
    private int totalRecords = 0;
    private int numBatches = 0;
    private int malformedCount = 0;

    public static void main(String[] args) {
        CLI cli = new CLI();
        cli.parseArgs(args);
        cli.run();
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
                case "--batch-size": batchSize = Integer.parseInt(args[++i]); break;
            }
        }
    }

    private void run() {
        printBanner();
        Scanner scanner = new Scanner(System.in);

        // Initialize MySQL store
        try {
            mysqlStore = new MySQLStore(mysqlHost, mysqlPort, mysqlUser, mysqlPassword);
            mysqlStore.initialize();
            printSuccess("Connected to MySQL at " + mysqlHost + ":" + mysqlPort);
        } catch (Exception e) {
            printError("Failed to connect to MySQL: " + e.getMessage());
            printInfo("Results will not be stored. Start MySQL and restart the tool.");
        }

        // Main REPL loop
        while (true) {
            System.out.print(BOLD + CYAN + "\nnasa> " + RESET);
            String input = scanner.nextLine().trim();

            if (input.isEmpty()) continue;

            String[] parts = input.split("\\s+", 2);
            String command = parts[0].toLowerCase();

            try {
                switch (command) {
                    case "use":
                        handleUse(parts.length > 1 ? parts[1].trim().toLowerCase() : "");
                        break;
                    case "load":
                        handleLoad(parts.length > 1 ? parts[1].trim() : "");
                        break;
                    case "query1":
                        handleQuery("query1", "Daily Traffic Summary");
                        break;
                    case "query2":
                        handleQuery("query2", "Top 20 Requested Resources");
                        break;
                    case "query3":
                        handleQuery("query3", "Hourly Error Analysis");
                        break;
                    case "run":
                        if (parts.length > 1 && parts[1].trim().equalsIgnoreCase("all")) {
                            handleRunAll();
                        } else {
                            printError("Unknown command. Did you mean 'run all'?");
                        }
                        break;
                    case "report":
                        handleReport(parts.length > 1 ? parts[1].trim() : "");
                        break;
                    case "status":
                        handleStatus();
                        break;
                    case "help":
                        printHelp();
                        break;
                    case "exit":
                    case "quit":
                        handleExit();
                        scanner.close();
                        return;
                    default:
                        printError("Unknown command: " + command + ". Type 'help' for available commands.");
                }
            } catch (UnsupportedOperationException e) {
                printError(e.getMessage());
            } catch (Exception e) {
                printError("Error: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    // ======================== Command Handlers ========================

    private void handleUse(String pipeline) throws Exception {
        if (pipeline.isEmpty()) {
            printError("Usage: use <pipeline>  (mapreduce | mongodb | pig | hive)");
            return;
        }

        // Clean up previous pipeline
        if (currentDb != null) {
            try { currentDb.cleanup(); } catch (Exception ignored) {}
        }
        dataLoaded = false;
        currentRunId = null;

        switch (pipeline) {
            case "mapreduce":
            case "mr":
                currentDb = new MapReduceDatabase();
                break;
            case "mongodb":
            case "mongo":
                currentDb = new MongoDBDatabase(mongoHost, mongoPort);
                break;
            case "pig":
                currentDb = new ApachePigDatabase();
                break;
            case "hive":
                currentDb = new ApacheHiveDatabase();
                break;
            default:
                printError("Unknown pipeline: " + pipeline);
                printInfo("Available: mapreduce (mr), mongodb (mongo), pig, hive");
                return;
        }

        currentDb.initialize();
        printSuccess("Switched to " + currentDb.getPipelineName() + " pipeline");
    }

    private void handleLoad(String batchSizeArg) throws Exception {
        if (currentDb == null) {
            printError("No pipeline selected. Use 'use <pipeline>' first.");
            return;
        }

        if (!batchSizeArg.isEmpty()) {
            try {
                batchSize = Integer.parseInt(batchSizeArg);
            } catch (NumberFormatException e) {
                printError("Invalid batch size: " + batchSizeArg);
                return;
            }
        }

        // Find dataset files
        File dataDirectory = new File(dataDir);
        if (!dataDirectory.exists() || !dataDirectory.isDirectory()) {
            printError("Dataset directory not found: " + dataDir);
            return;
        }

        List<File> files = new ArrayList<>();
        File[] dataFiles = dataDirectory.listFiles((dir, name) ->
                name.endsWith(".gz") || name.endsWith(".log") || name.endsWith(".txt"));
        if (dataFiles == null || dataFiles.length == 0) {
            printError("No log files found in " + dataDir);
            return;
        }
        Arrays.sort(dataFiles);
        files.addAll(Arrays.asList(dataFiles));

        printInfo("Loading " + files.size() + " file(s) with batch size " + batchSize + "...");

        // Generate run ID
        currentRunId = currentDb.getPipelineName().toLowerCase() + "-" +
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));

        // Re-initialize pipeline for a clean load
        currentDb.cleanup();
        currentDb.initialize();

        // Parse and load in batches
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

            // Progress indicator
            if (batchId % 50 == 0 || batchId == batches.size()) {
                System.out.printf("  %sBatch %d/%d (%,d records so far)%s%n",
                        DIM, batchId, batches.size(), totalRecords, RESET);
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        malformedCount = parser.getMalformedCount();

        // Update pipeline metadata
        currentDb.setBatchSize(batchSize);
        currentDb.setRunId(currentRunId);
        currentDb.setTotalRecords(totalRecords);
        currentDb.setNumBatches(numBatches);
        currentDb.setMalformedCount(malformedCount);

        dataLoaded = true;

        printSuccess(String.format("Loaded %,d records in %d batches (%.1fs)",
                totalRecords, numBatches, elapsed / 1000.0));
        printInfo(String.format("Avg batch size: %.2f | Malformed records: %,d | Run ID: %s",
                currentDb.getAvgBatchSize(), malformedCount, currentRunId));
    }

    private void handleQuery(String queryName, String queryLabel) throws Exception {
        if (currentDb == null) {
            printError("No pipeline selected. Use 'use <pipeline>' first.");
            return;
        }
        if (!dataLoaded) {
            printError("No data loaded. Use 'load [batch_size]' first.");
            return;
        }

        printInfo("Running " + queryLabel + " on " + currentDb.getPipelineName() + "...");

        long startTime = System.currentTimeMillis();
        List<Map<String, Object>> results;

        switch (queryName) {
            case "query1":
                results = currentDb.executeDailyTrafficSummary();
                break;
            case "query2":
                results = currentDb.executeTopRequestedResources();
                break;
            case "query3":
                results = currentDb.executeHourlyErrorAnalysis();
                break;
            default:
                printError("Unknown query: " + queryName);
                return;
        }

        double executionTime = (System.currentTimeMillis() - startTime) / 1000.0;

        // Display results
        printQueryResults(queryName, queryLabel, results, executionTime);

        // Store in MySQL
        if (mysqlStore != null) {
            try {
                mysqlStore.storeResults(queryName, currentDb.getPipelineName(), currentRunId,
                        0, executionTime, batchSize, totalRecords, numBatches,
                        currentDb.getAvgBatchSize(), malformedCount, results);
                printSuccess(String.format("Results stored in MySQL (run_id: %s)", currentRunId));
            } catch (Exception e) {
                printError("Failed to store results in MySQL: " + e.getMessage());
            }
        }
    }

    private void handleRunAll() throws Exception {
        handleQuery("query1", "Daily Traffic Summary");
        handleQuery("query2", "Top 20 Requested Resources");
        handleQuery("query3", "Hourly Error Analysis");
    }

    private void handleReport(String runIdFilter) throws Exception {
        if (mysqlStore == null) {
            printError("MySQL not connected. Cannot retrieve reports.");
            return;
        }

        List<Map<String, Object>> records;
        if (!runIdFilter.isEmpty()) {
            records = mysqlStore.getResultsByRunId(runIdFilter);
        } else {
            records = mysqlStore.getRecentResults(10);
        }

        if (records.isEmpty()) {
            printInfo("No results found." + (runIdFilter.isEmpty() ? "" : " Run ID: " + runIdFilter));
            return;
        }

        System.out.println();
        System.out.println(BOLD + "═══════════════════════════════════════════════════════════════" + RESET);
        System.out.println(BOLD + CYAN + "  Stored Query Results" + RESET);
        System.out.println(BOLD + "═══════════════════════════════════════════════════════════════" + RESET);

        for (Map<String, Object> record : records) {
            System.out.println();
            System.out.printf("  %sPipeline:%s   %-15s %sQuery:%s     %s%n",
                    BOLD, RESET, record.get("pipeline_name"),
                    BOLD, RESET, record.get("query_name"));
            System.out.printf("  %sRun ID:%s    %-15s %sExec Time:%s %.3fs%n",
                    BOLD, RESET, record.get("run_id"),
                    BOLD, RESET, record.get("execution_time"));
            System.out.printf("  %sBatch Size:%s %-15s %sBatches:%s   %s%n",
                    BOLD, RESET, record.get("batch_size"),
                    BOLD, RESET, record.get("num_batches"));
            System.out.printf("  %sTotal Recs:%s %-15s %sAvg Batch:%s %.2f%n",
                    BOLD, RESET, record.get("total_records"),
                    BOLD, RESET, record.get("avg_batch_size"));
            System.out.printf("  %sMalformed:%s  %-15s %sTime:%s      %s%n",
                    BOLD, RESET, record.get("malformed_count"),
                    BOLD, RESET, record.get("executed_at"));
            System.out.println("  " + DIM + "─────────────────────────────────────────────────" + RESET);
        }
    }

    private void handleStatus() {
        System.out.println();
        System.out.println(BOLD + "═══════════════════════════════════════════════" + RESET);
        System.out.println(BOLD + CYAN + "  Current Status" + RESET);
        System.out.println(BOLD + "═══════════════════════════════════════════════" + RESET);

        String pipelineStr = currentDb != null ? currentDb.getPipelineName() : "(none)";
        System.out.printf("  %sPipeline:%s     %s%n", BOLD, RESET, pipelineStr);
        System.out.printf("  %sData Loaded:%s  %s%n", BOLD, RESET, dataLoaded ? GREEN + "Yes" + RESET : RED + "No" + RESET);
        System.out.printf("  %sBatch Size:%s   %,d%n", BOLD, RESET, batchSize);

        if (dataLoaded) {
            System.out.printf("  %sTotal Records:%s %,d%n", BOLD, RESET, totalRecords);
            System.out.printf("  %sBatches:%s      %d%n", BOLD, RESET, numBatches);
            System.out.printf("  %sAvg Batch:%s    %.2f%n", BOLD, RESET,
                    numBatches > 0 ? (double) totalRecords / numBatches : 0.0);
            System.out.printf("  %sMalformed:%s    %,d%n", BOLD, RESET, malformedCount);
            System.out.printf("  %sRun ID:%s       %s%n", BOLD, RESET, currentRunId);
        }

        System.out.printf("  %sMySQL:%s        %s%n", BOLD, RESET,
                mysqlStore != null ? GREEN + "Connected" + RESET : RED + "Disconnected" + RESET);
        System.out.println();
    }

    private void handleExit() {
        if (currentDb != null) {
            try { currentDb.cleanup(); } catch (Exception ignored) {}
        }
        if (mysqlStore != null) {
            mysqlStore.close();
        }
        System.out.println(GREEN + "Goodbye!" + RESET);
    }

    // ======================== Display Helpers ========================

    private void printQueryResults(String queryName, String queryLabel,
                                    List<Map<String, Object>> results, double execTime) {
        System.out.println();
        System.out.println(BOLD + "═══════════════════════════════════════════════════════════════════════════════" + RESET);
        System.out.printf(BOLD + CYAN + "  %s  " + RESET + DIM + "(%.3fs on %s)%n" + RESET,
                queryLabel, execTime, currentDb.getPipelineName());
        System.out.println(BOLD + "═══════════════════════════════════════════════════════════════════════════════" + RESET);

        if (results.isEmpty()) {
            printInfo("No results returned.");
            return;
        }

        // Get column headers from first row
        List<String> columns = new ArrayList<>(results.get(0).keySet());

        // Calculate column widths
        int[] widths = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            widths[i] = columns.get(i).length();
        }
        for (Map<String, Object> row : results) {
            int i = 0;
            for (String col : columns) {
                String val = formatValue(row.get(col));
                widths[i] = Math.max(widths[i], val.length());
                i++;
            }
        }

        // Print header
        StringBuilder header = new StringBuilder("  ");
        StringBuilder separator = new StringBuilder("  ");
        for (int i = 0; i < columns.size(); i++) {
            header.append(String.format(BOLD + "%-" + widths[i] + "s" + RESET, columns.get(i)));
            separator.append("-".repeat(widths[i]));
            if (i < columns.size() - 1) {
                header.append(" │ ");
                separator.append("─┼─");
            }
        }
        System.out.println(header);
        System.out.println(separator);

        // Print rows (limit display to 30 rows, show count if more)
        int displayLimit = queryName.equals("query2") ? 20 : 30;
        int shown = 0;
        for (Map<String, Object> row : results) {
            if (shown >= displayLimit) break;
            StringBuilder line = new StringBuilder("  ");
            int i = 0;
            for (String col : columns) {
                String val = formatValue(row.get(col));
                line.append(String.format("%-" + widths[i] + "s", val));
                if (i < columns.size() - 1) line.append(" │ ");
                i++;
            }
            System.out.println(line);
            shown++;
        }

        if (results.size() > displayLimit) {
            System.out.printf("  %s... and %d more rows%s%n", DIM, results.size() - displayLimit, RESET);
        }

        System.out.printf("%n  %sTotal: %d rows%s%n", DIM, results.size(), RESET);
    }

    private String formatValue(Object val) {
        if (val == null) return "null";
        if (val instanceof Double) {
            return String.format("%.6f", (Double) val);
        }
        if (val instanceof Long) {
            return String.format("%,d", (Long) val);
        }
        if (val instanceof Integer) {
            return String.format("%,d", (Integer) val);
        }
        return val.toString();
    }

    // ======================== UI Helpers ========================

    private void printBanner() {
        System.out.println();
        System.out.println(BOLD + CYAN + "  ╔═══════════════════════════════════════════════╗" + RESET);
        System.out.println(BOLD + CYAN + "  ║     NASA Log Analytics Tool  v1.0             ║" + RESET);
        System.out.println(BOLD + CYAN + "  ║     NoSQL End Semester Project                ║" + RESET);
        System.out.println(BOLD + CYAN + "  ╚═══════════════════════════════════════════════╝" + RESET);
        System.out.println(DIM + "  Type 'help' for available commands" + RESET);
        System.out.println();
    }

    private void printHelp() {
        System.out.println();
        System.out.println(BOLD + "  Available Commands:" + RESET);
        System.out.println("  ─────────────────────────────────────────────────────");
        System.out.printf("  %suse <pipeline>%s   Switch pipeline (mapreduce/mr, mongodb/mongo, pig, hive)%n", BOLD, RESET);
        System.out.printf("  %sload [size]%s      Load dataset with optional batch size (default: %d)%n", BOLD, RESET, batchSize);
        System.out.printf("  %squery1%s           Daily Traffic Summary%n", BOLD, RESET);
        System.out.printf("  %squery2%s           Top 20 Requested Resources%n", BOLD, RESET);
        System.out.printf("  %squery3%s           Hourly Error Analysis%n", BOLD, RESET);
        System.out.printf("  %srun all%s          Execute all 3 queries%n", BOLD, RESET);
        System.out.printf("  %sreport [run_id]%s  Show results from MySQL%n", BOLD, RESET);
        System.out.printf("  %sstatus%s           Show current pipeline and load info%n", BOLD, RESET);
        System.out.printf("  %shelp%s             Show this help message%n", BOLD, RESET);
        System.out.printf("  %sexit%s             Exit the tool%n", BOLD, RESET);
        System.out.println();
    }

    private void printSuccess(String msg) {
        System.out.println("  " + GREEN + "✓ " + msg + RESET);
    }

    private void printError(String msg) {
        System.out.println("  " + RED + "✗ " + msg + RESET);
    }

    private void printInfo(String msg) {
        System.out.println("  " + YELLOW + "ℹ " + msg + RESET);
    }
}
