package com.etl;

import com.etl.cli.InteractiveMode;
import com.etl.config.AppConfig;
import com.etl.db.SchemaInitializer;
import com.etl.orchestrator.Orchestrator;
import com.etl.report.ReportGenerator;

import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * CLI entry point for the NASA Log Analytics ETL Tool.
 *
 * Supports two modes:
 * 1. Non-interactive: java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar <command> [options]
 * 2. Interactive: java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar (no args or --interactive)
 *
 * Commands: run, report, list-runs, compare, init-db, help
 */
public class Main {

    private static final String LINE = repeat("=", 70);

    public static void main(String[] args) {
        try {
            // No arguments or --interactive → enter interactive mode
            if (args.length == 0 || "--interactive".equals(args[0])) {
                InteractiveMode.start();
                return;
            }

            String command = args[0];
            Map<String, String> flags = parseFlags(args, 1);

            switch (command) {
                case "run":
                    handleRun(flags);
                    break;
                case "report":
                    handleReport(flags);
                    break;
                case "list-runs":
                    handleListRuns(flags);
                    break;
                case "compare":
                    handleCompare(flags);
                    break;
                case "init-db":
                    handleInitDb(flags);
                    break;
                case "help":
                case "--help":
                case "-h":
                    printHelp();
                    break;
                default:
                    System.err.println("Error: Unknown command: '" + command + "'");
                    System.err.println("Run 'java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar help' for usage.");
                    System.exit(1);
            }

        } catch (SQLException e) {
            System.err.println("Error: Cannot connect to MySQL at localhost:3306/etl_results");
            System.err.println("  Cause: " + e.getMessage());
            System.err.println("  Please verify MySQL is running and credentials are correct.");
            System.err.println("  Run 'java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar init-db' to create the schema.");
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    // =====================================================================
    // Flag parser
    // =====================================================================

    /**
     * Parses flags from args starting at the given index.
     * Supports: --key value (two-token flags) and --flag (boolean flags).
     *
     * Boolean flags (no value expected): --latest, --reset, --interactive
     * All others expect a value token after the key.
     */
    private static Map<String, String> parseFlags(String[] args, int startIndex) {
        Set<String> booleanFlags = new HashSet<String>(
            Arrays.asList("--latest", "--reset", "--interactive")
        );
        Map<String, String> flags = new LinkedHashMap<String, String>();
        int i = startIndex;
        while (i < args.length) {
            String key = args[i];
            if (!key.startsWith("--")) {
                System.err.println("Unexpected argument: " + key);
                System.exit(1);
            }
            if (booleanFlags.contains(key)) {
                flags.put(key, "true");
                i++;
            } else {
                if (i + 1 >= args.length) {
                    System.err.println("Missing value for flag: " + key);
                    System.exit(1);
                }
                flags.put(key, args[i + 1]);
                i += 2;
            }
        }
        return flags;
    }

    // =====================================================================
    // Command handlers
    // =====================================================================

    private static void handleRun(Map<String, String> flags) throws Exception {
        // 1. Extract and validate --pipeline
        String pipeline = flags.get("--pipeline");
        if (pipeline == null) {
            exitWithError("Missing required flag: --pipeline\n" +
                "Allowed values: pig, mapreduce, mongodb, hive");
        }
        Set<String> validPipelines = new HashSet<String>(
            Arrays.asList("pig", "mapreduce", "mongodb", "hive"));
        if (!validPipelines.contains(pipeline)) {
            exitWithError("Invalid pipeline: '" + pipeline + "'\n" +
                "Allowed values: pig, mapreduce, mongodb, hive");
        }

        // 2. Extract and validate --batch-strategy
        String batchStrategy = flags.get("--batch-strategy");
        if (batchStrategy == null) {
            exitWithError("Missing required flag: --batch-strategy\n" +
                "Allowed values: record_count, file_based");
        }
        Set<String> validStrategies = new HashSet<String>(
            Arrays.asList("record_count", "file_based"));
        if (!validStrategies.contains(batchStrategy)) {
            exitWithError("Invalid batch strategy: '" + batchStrategy + "'\n" +
                "Allowed values: record_count, file_based");
        }

        // 3. Extract and validate --batch-size (conditional)
        int batchSize = 0;
        if ("record_count".equals(batchStrategy)) {
            String batchSizeStr = flags.get("--batch-size");
            if (batchSizeStr == null) {
                exitWithError("--batch-size is required when using record_count batch strategy.");
            }
            try {
                batchSize = Integer.parseInt(batchSizeStr);
            } catch (NumberFormatException e) {
                exitWithError("--batch-size must be a positive integer. Got: '" + batchSizeStr + "'");
            }
            if (batchSize <= 0) {
                exitWithError("--batch-size must be a positive integer. Got: " + batchSize);
            }
        } else if (flags.containsKey("--batch-size")) {
            System.out.println("Warning: --batch-size is ignored for the '" +
                batchStrategy + "' batch strategy.");
        }

        // 4. Extract --query (optional, defaults to "all")
        String query = flags.get("--query");
        if (query == null) {
            query = "all";
        }
        Set<String> validQueries = new HashSet<String>(
            Arrays.asList("query1", "query2", "query3", "all"));
        if (!validQueries.contains(query)) {
            exitWithError("Invalid query: '" + query + "'\n" +
                "Allowed values: query1, query2, query3, all");
        }

        // 5. Extract --data-dir (optional, defaults to "./data/")
        String dataDir = flags.get("--data-dir");
        if (dataDir == null) {
            dataDir = AppConfig.DEFAULT_DATA_DIR;
        }
        File dataDirFile = new File(dataDir);
        if (!dataDirFile.isDirectory()) {
            exitWithError("Data directory does not exist: " + dataDir);
        }
        File[] logFiles = dataDirFile.listFiles(new java.io.FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("NASA_access_log_");
            }
        });
        if (logFiles == null || logFiles.length == 0) {
            exitWithError("No NASA log files found in '" + dataDir +
                "'. Expected files: NASA_access_log_Jul95, NASA_access_log_Aug95");
        }

        // 6. Initialize schema silently
        SchemaInitializer.initialize();

        // 7. Print run header
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

        // 8. Execute
        Orchestrator orchestrator = new Orchestrator();
        int runId = orchestrator.executeRun(pipeline, query, batchStrategy, batchSize, dataDir);

        // 9. Print footer
        System.out.println();
        System.out.println(LINE);
        System.out.println("RUN COMPLETE");
        System.out.println(LINE);
        System.out.println("Results stored in MySQL. To view the report:");
        System.out.println("  java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar report --run-id " + runId);
        System.out.println(LINE);
    }

    private static void handleReport(Map<String, String> flags) throws Exception {
        boolean latest = flags.containsKey("--latest");
        String runIdStr = flags.get("--run-id");
        String pipeline = flags.get("--pipeline");

        if (!latest && runIdStr == null) {
            exitWithError("Specify --run-id <N> or --latest");
        }

        if (latest) {
            ReportGenerator.generateReport(null, true, pipeline);
        } else {
            int runId;
            try {
                runId = Integer.parseInt(runIdStr);
            } catch (NumberFormatException e) {
                exitWithError("--run-id must be an integer. Got: '" + runIdStr + "'");
                return; // Unreachable, but satisfies compiler
            }
            ReportGenerator.generateReport(runId, false, null);
        }
    }

    private static void handleListRuns(Map<String, String> flags) throws Exception {
        String pipeline = flags.get("--pipeline");
        String limitStr = flags.get("--limit");
        Integer limit = null;
        if (limitStr != null) {
            try {
                limit = Integer.parseInt(limitStr);
            } catch (NumberFormatException e) {
                exitWithError("--limit must be an integer. Got: '" + limitStr + "'");
            }
        }
        ReportGenerator.listRuns(pipeline, limit);
    }

    private static void handleCompare(Map<String, String> flags) throws Exception {
        String batchStrategy = flags.get("--batch-strategy");
        String runIds = flags.get("--run-ids");

        if (batchStrategy == null && runIds == null) {
            exitWithError("Specify --batch-strategy <name> or --run-ids <id1,id2,...>");
        }

        ReportGenerator.comparePipelines(batchStrategy, runIds);
    }

    private static void handleInitDb(Map<String, String> flags) throws Exception {
        boolean reset = flags.containsKey("--reset");

        if (reset) {
            SchemaInitializer.reset();
        } else {
            SchemaInitializer.initialize();
        }
    }

    // =====================================================================
    // Utility methods
    // =====================================================================

    private static void exitWithError(String message) {
        System.err.println("Error: " + message);
        System.exit(1);
    }

    private static void printHelp() {
        System.out.println("NASA Log Analytics ETL Tool v1.0");
        System.out.println("================================");
        System.out.println();
        System.out.println("Usage: java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar <command> [options]");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  (no command)     Interactive mode -- guided step-by-step execution");
        System.out.println("  run              Execute an ETL pipeline");
        System.out.println("  report           Display results from a past run");
        System.out.println("  list-runs        Show summary of all past runs");
        System.out.println("  compare          Compare results across pipelines");
        System.out.println("  init-db          Initialize or reset MySQL schema");
        System.out.println("  help             Show this help message");
        System.out.println();
        System.out.println("Run options:");
        System.out.println("  --pipeline <name>         Pipeline: pig, mapreduce, mongodb, hive");
        System.out.println("  --query <name>            Query: query1, query2, query3, all (default: all)");
        System.out.println("  --batch-strategy <name>   Strategy: record_count, file_based");
        System.out.println("  --batch-size <N>          Records per batch (required for record_count)");
        System.out.println("  --data-dir <path>         Log file directory (default: ./data/)");
        System.out.println();
        System.out.println("Report options:");
        System.out.println("  --run-id <N>              Report on a specific run");
        System.out.println("  --latest                  Report on the most recent run");
        System.out.println("  --pipeline <name>         Filter --latest to a specific pipeline");
        System.out.println();
        System.out.println("List-runs options:");
        System.out.println("  --pipeline <name>         Filter by pipeline");
        System.out.println("  --limit <N>               Show only the last N runs");
        System.out.println();
        System.out.println("Compare options:");
        System.out.println("  --batch-strategy <name>   Compare latest run per pipeline for this strategy");
        System.out.println("  --run-ids <id1,id2,...>    Compare specific runs by ID");
        System.out.println();
        System.out.println("Init-db options:");
        System.out.println("  --reset                   Drop and recreate all tables (destructive)");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline mongodb --query all --batch-strategy file_based");
        System.out.println("  java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline pig --query query1 --batch-strategy record_count --batch-size 100000");
        System.out.println("  java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar report --latest");
        System.out.println("  java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar report --latest --pipeline mapreduce");
        System.out.println("  java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar list-runs");
        System.out.println("  java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar compare --batch-strategy file_based");
        System.out.println("  java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar init-db");
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
