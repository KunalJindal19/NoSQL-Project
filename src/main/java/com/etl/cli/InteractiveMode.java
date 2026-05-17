package com.etl.cli;

import com.etl.db.SchemaInitializer;
import com.etl.orchestrator.Orchestrator;
import com.etl.report.ReportGenerator;

import java.io.File;
import java.util.Scanner;

/**
 * Interactive CLI mode that provides a guided step-by-step menu.
 * Launched when the user runs the tool with no arguments or with --interactive.
 * Uses the same backend methods as the non-interactive command handlers.
 */
public class InteractiveMode {

    private static final Scanner scanner = new Scanner(System.in);
    private static final String LINE = repeat("=", 70);

    public static void start() throws Exception {
        System.out.println(LINE);
        System.out.println("NASA Log Analytics ETL Tool -- Interactive Mode");
        System.out.println(LINE);

        while (true) {
            System.out.println();
            System.out.println("What would you like to do?");
            System.out.println();
            System.out.println("  [1] Run an ETL pipeline");
            System.out.println("  [2] View report for a past run");
            System.out.println("  [3] List all past runs");
            System.out.println("  [4] Compare pipelines");
            System.out.println("  [5] Initialize MySQL schema");
            System.out.println("  [6] Exit");
            System.out.println();

            int choice = readInt("Enter choice (1-6): ", 1, 6);

            switch (choice) {
                case 1:
                    runPipeline();
                    break;
                case 2:
                    viewReport();
                    break;
                case 3:
                    listRuns();
                    break;
                case 4:
                    comparePipelines();
                    break;
                case 5:
                    initSchema();
                    break;
                case 6:
                    System.out.println("Goodbye.");
                    return;
                default:
                    break;
            }

            System.out.println();
            System.out.print("Press Enter to return to the main menu...");
            scanner.nextLine();
        }
    }

    private static void runPipeline() throws Exception {
        System.out.println();
        System.out.println("--- Pipeline Selection ---");
        System.out.println("Available pipelines:");
        System.out.println("  [1] pig        (Apache Pig, local mode)");
        System.out.println("  [2] mapreduce  (Hadoop MapReduce, local standalone)");
        System.out.println("  [3] mongodb    (MongoDB, standalone instance)");
        System.out.println("  [4] hive       (Apache Hive, local mode with embedded metastore)");
        System.out.println();
        int pipelineChoice = readInt("Select pipeline (1-4): ", 1, 4);
        String[] pipelineNames = {"pig", "mapreduce", "mongodb", "hive"};
        String pipeline = pipelineNames[pipelineChoice - 1];

        System.out.println();
        System.out.println("--- Query Selection ---");
        System.out.println("Available queries:");
        System.out.println("  [1] query1     (Daily Traffic Summary)");
        System.out.println("  [2] query2     (Top Requested Resources)");
        System.out.println("  [3] query3     (Hourly Error Analysis)");
        System.out.println("  [4] all        (Run all three queries)");
        System.out.println();
        int queryChoice = readInt("Select query (1-4): ", 1, 4);
        String[] queryNames = {"query1", "query2", "query3", "all"};
        String query = queryNames[queryChoice - 1];

        System.out.println();
        System.out.println("--- Batch Strategy Selection ---");
        System.out.println("Available strategies:");
        System.out.println("  [1] record_count  (Fixed number of records per batch)");
        System.out.println("  [2] file_based    (One batch per input file -- July=Batch1, August=Batch2)");
        System.out.println();
        int strategyChoice = readInt("Select strategy (1-2): ", 1, 2);
        String[] strategyNames = {"record_count", "file_based"};
        String batchStrategy = strategyNames[strategyChoice - 1];

        int batchSize = 0;
        if ("record_count".equals(batchStrategy)) {
            System.out.println();
            System.out.println("--- Batch Size ---");
            batchSize = readInt("Enter number of records per batch: ", 1, Integer.MAX_VALUE);
        }

        System.out.println();
        System.out.println("--- Data Directory ---");
        System.out.print("Enter path to log files directory (press Enter for default ./data/): ");
        String dataDir = scanner.nextLine().trim();
        if (dataDir.isEmpty()) {
            dataDir = "./data/";
        }

        // Validate data directory
        File dataDirFile = new File(dataDir);
        if (!dataDirFile.isDirectory()) {
            System.out.println("Error: Data directory does not exist: " + dataDir);
            return;
        }
        File[] logFiles = dataDirFile.listFiles(new java.io.FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("NASA_access_log_");
            }
        });
        if (logFiles == null || logFiles.length == 0) {
            System.out.println("Error: No NASA log files found in '" + dataDir +
                "'. Expected files: NASA_access_log_Jul95, NASA_access_log_Aug95");
            return;
        }

        // Confirm
        System.out.println();
        System.out.println("--- Confirm ---");
        System.out.println("  Pipeline:       " + pipeline);
        System.out.println("  Query:          " + query);
        System.out.println("  Batch Strategy: " + batchStrategy);
        if ("record_count".equals(batchStrategy)) {
            System.out.println("  Batch Size:     " + batchSize);
        }
        System.out.println("  Data Directory: " + dataDir);
        System.out.println();

        String confirm = readLine("Proceed? (y/n): ").toLowerCase();
        if (!"y".equals(confirm) && !"yes".equals(confirm)) {
            System.out.println("Cancelled.");
            return;
        }

        System.out.println();
        String queryDisplay = "all".equals(query) ? "all (query1, query2, query3)" : query;
        System.out.println(LINE);
        System.out.println("NASA Log Analytics ETL Tool");
        System.out.println(LINE);
        System.out.println("Pipeline:       " + pipeline);
        System.out.println("Query:          " + queryDisplay);
        System.out.println("Batch Strategy: " + batchStrategy);
        if ("record_count".equals(batchStrategy)) {
            System.out.println("Batch Size:     " + batchSize);
        }
        System.out.println("Data Directory: " + dataDir);
        System.out.println(LINE);
        System.out.println();
        System.out.println("Reading input files and splitting into batches...");

        Orchestrator orchestrator = new Orchestrator();
        int runId = orchestrator.executeRun(pipeline, query, batchStrategy, batchSize, dataDir);

        System.out.println();
        System.out.println("To view the full report, select option [2] from the main menu");
        System.out.println("and enter Run ID: " + runId);
    }

    private static void viewReport() throws Exception {
        System.out.println();
        System.out.println("--- Report ---");
        String input = readLine("Enter Run ID (or 'latest' for the most recent run): ");

        if ("latest".equalsIgnoreCase(input)) {
            String pipelineFilter = readLine("Filter by pipeline? (Enter pipeline name or press Enter to skip): ");
            if (pipelineFilter.isEmpty()) {
                pipelineFilter = null;
            }
            ReportGenerator.generateReport(null, true, pipelineFilter);
        } else {
            try {
                int runId = Integer.parseInt(input);
                ReportGenerator.generateReport(runId, false, null);
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a number or 'latest'.");
            }
        }
    }

    private static void listRuns() throws Exception {
        System.out.println();
        System.out.println("--- Past Runs ---");
        ReportGenerator.listRuns(null, null);
    }

    private static void comparePipelines() throws Exception {
        System.out.println();
        System.out.println("--- Compare Pipelines ---");
        System.out.println("Compare by:");
        System.out.println("  [1] Batch strategy (latest run per pipeline)");
        System.out.println("  [2] Specific run IDs");
        System.out.println();

        int compareChoice = readInt("Select (1-2): ", 1, 2);

        if (compareChoice == 1) {
            System.out.println();
            System.out.println("Available batch strategies: record_count, file_based");
            String batchStrategy = readLine("Enter batch strategy: ");
            if (!"record_count".equals(batchStrategy) && !"file_based".equals(batchStrategy)) {
                System.out.println("Invalid batch strategy.");
                return;
            }
            ReportGenerator.comparePipelines(batchStrategy, null);
        } else {
            String runIds = readLine("Enter run IDs (comma-separated, e.g. 1,2,3,4): ");
            ReportGenerator.comparePipelines(null, runIds);
        }
    }

    private static void initSchema() throws Exception {
        System.out.println();
        System.out.println("--- Initialize MySQL Schema ---");
        System.out.println("  [1] Create tables (safe, preserves existing data)");
        System.out.println("  [2] Reset all tables (DELETES all existing data)");
        System.out.println();

        int schemaChoice = readInt("Select (1-2): ", 1, 2);

        if (schemaChoice == 1) {
            SchemaInitializer.initialize();
        } else {
            SchemaInitializer.reset();
        }
    }

    // =====================================================================
    // Helper methods for input reading with validation
    // =====================================================================

    /**
     * Reads an integer within a range, re-prompting on invalid input.
     * Never crashes — keeps looping until valid input is given.
     */
    private static int readInt(String prompt, int min, int max) {
        while (true) {
            System.out.print(prompt);
            System.out.flush();
            String input = scanner.nextLine().trim();
            try {
                int value = Integer.parseInt(input);
                if (value >= min && value <= max) {
                    return value;
                }
                if (max == Integer.MAX_VALUE) {
                    System.out.println("Please enter a number >= " + min + ".");
                } else {
                    System.out.println("Invalid selection. Please enter a number between " + min + " and " + max + ".");
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a number.");
            }
        }
    }

    /**
     * Reads a trimmed line from stdin.
     */
    private static String readLine(String prompt) {
        System.out.print(prompt);
        System.out.flush();
        return scanner.nextLine().trim();
    }

    /**
     * Java 8 compatible replacement for String.repeat().
     */
    private static String repeat(String s, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(s);
        }
        return sb.toString();
    }
}
