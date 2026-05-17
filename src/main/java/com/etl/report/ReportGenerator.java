package com.etl.report;

import com.etl.db.ConnectionManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads results from MySQL and displays formatted output.
 * Shows execution metadata, batch details, and query results.
 * Also provides list-runs and compare-pipelines functionality.
 */
public class ReportGenerator {

    private static final String LINE = repeat("=", 70);
    private static final String DASH = repeat("-", 70);

    /**
     * Generates a full report for a specific run or the latest run.
     *
     * @param runId          specific run ID, or null if using latest
     * @param latest         if true, pick the most recent run
     * @param pipelineFilter if non-null and latest=true, filter to this pipeline
     */
    public static void generateReport(Integer runId, boolean latest, String pipelineFilter)
            throws SQLException {
        try (Connection conn = ConnectionManager.getConnection()) {
            String sql;
            if (latest) {
                if (pipelineFilter != null && !pipelineFilter.isEmpty()) {
                    sql = "SELECT * FROM run_metadata WHERE pipeline_name = '" +
                        pipelineFilter.replace("'", "''") +
                        "' ORDER BY run_id DESC LIMIT 1";
                } else {
                    sql = "SELECT * FROM run_metadata ORDER BY run_id DESC LIMIT 1";
                }
            } else {
                sql = "SELECT * FROM run_metadata WHERE run_id = " + runId;
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (!rs.next()) {
                    if (latest && pipelineFilter != null) {
                        System.out.println("Error: No runs found for pipeline '" + pipelineFilter + "'");
                    } else if (runId != null) {
                        System.out.println("Error: No run found with ID " + runId);
                    } else {
                        System.out.println("Error: No runs found.");
                    }
                    return;
                }

                int rid = rs.getInt("run_id");
                String pipelineName = rs.getString("pipeline_name");
                String queryName = rs.getString("query_name");
                String batchStrategy = rs.getString("batch_strategy");
                Object configuredBatch = rs.getObject("configured_batch_size");
                int totalRecords = rs.getInt("total_records_processed");
                int totalMalformed = rs.getInt("total_malformed_records");
                int totalBatches = rs.getInt("total_batches");
                double avgBatchSize = rs.getDouble("average_batch_size");
                double runtimeSeconds = rs.getDouble("runtime_seconds");
                String executedAt = rs.getString("execution_timestamp");

                System.out.println(LINE);
                System.out.println("EXECUTION METADATA");
                System.out.println(LINE);
                System.out.println("  Run ID:              " + rid);
                System.out.println("  Pipeline:            " + pipelineName);
                System.out.println("  Query:               " + queryName);
                System.out.println("  Batch Strategy:      " + batchStrategy);
                System.out.println("  Batch Size (config): " +
                    (configuredBatch != null ? String.valueOf(rs.getInt("configured_batch_size")) : "N/A"));
                System.out.println("  Total Records:       " + totalRecords);
                System.out.println("  Malformed Records:   " + totalMalformed);
                System.out.println("  Total Batches:       " + totalBatches);
                System.out.println("  Avg Batch Size:      " + String.format("%.2f", avgBatchSize));
                System.out.println("  Runtime (seconds):   " + String.format("%.3f", runtimeSeconds));
                System.out.println("  Executed At:         " + executedAt);
                System.out.println();

                // Display batch metadata
                printBatchMetadata(conn, rid);

                // Display query results
                if ("all".equals(queryName) || "query1".equals(queryName)) {
                    printQueryResults(conn, rid, "query1_daily_traffic",
                        "QUERY 1: Daily Traffic Summary", 50);
                }
                if ("all".equals(queryName) || "query2".equals(queryName)) {
                    printQueryResults(conn, rid, "query2_top_resources",
                        "QUERY 2: Top Requested Resources", 20);
                }
                if ("all".equals(queryName) || "query3".equals(queryName)) {
                    printQueryResults(conn, rid, "query3_hourly_errors",
                        "QUERY 3: Hourly Error Analysis", 50);
                }
            }
        }
    }

    /**
     * Lists all past runs from run_metadata table.
     *
     * @param pipelineFilter optional filter by pipeline name
     * @param limit          optional limit on number of rows, null for all
     */
    public static void listRuns(String pipelineFilter, Integer limit) throws SQLException {
        try (Connection conn = ConnectionManager.getConnection()) {
            StringBuilder sql = new StringBuilder("SELECT * FROM run_metadata");
            List<String> conditions = new ArrayList<String>();

            if (pipelineFilter != null && !pipelineFilter.isEmpty()) {
                conditions.add("pipeline_name = ?");
            }

            if (!conditions.isEmpty()) {
                sql.append(" WHERE ").append(conditions.get(0));
            }

            sql.append(" ORDER BY run_id ASC");

            if (limit != null && limit > 0) {
                sql.append(" LIMIT ").append(limit);
            }

            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int paramIdx = 1;
                if (pipelineFilter != null && !pipelineFilter.isEmpty()) {
                    ps.setString(paramIdx++, pipelineFilter);
                }

                try (ResultSet rs = ps.executeQuery()) {
                    System.out.println(LINE);
                    System.out.println("PAST RUNS");
                    System.out.println(LINE);

                    System.out.printf("%-8s %-13s %-8s %-16s %-12s %-11s %-11s %-9s %-12s %-20s%n",
                        "Run ID", "Pipeline", "Query", "Batch Strategy", "Batch Size",
                        "Records", "Malformed", "Batches", "Runtime(s)", "Timestamp");
                    System.out.println(repeat("-", 120));

                    int count = 0;
                    while (rs.next()) {
                        count++;
                        Object configuredBatch = rs.getObject("configured_batch_size");
                        String batchSizeStr = configuredBatch != null ?
                            String.valueOf(rs.getInt("configured_batch_size")) : "N/A";
                        System.out.printf("%-8d %-13s %-8s %-16s %-12s %-11d %-11d %-9d %-12.3f %-20s%n",
                            rs.getInt("run_id"),
                            rs.getString("pipeline_name"),
                            rs.getString("query_name"),
                            rs.getString("batch_strategy"),
                            batchSizeStr,
                            rs.getInt("total_records_processed"),
                            rs.getInt("total_malformed_records"),
                            rs.getInt("total_batches"),
                            rs.getDouble("runtime_seconds"),
                            rs.getString("execution_timestamp"));
                    }

                    System.out.println();
                    System.out.println("Total: " + count + " runs");
                }
            }
        }
    }

    /**
     * Compares results and runtimes across pipelines.
     *
     * @param batchStrategy compare latest run per pipeline for this strategy (mutually exclusive with runIdsStr)
     * @param runIdsStr     comma-separated list of run IDs to compare (mutually exclusive with batchStrategy)
     */
    public static void comparePipelines(String batchStrategy, String runIdsStr) throws SQLException {
        try (Connection conn = ConnectionManager.getConnection()) {
            // Collect the runs to compare
            List<RunInfo> runs = new ArrayList<RunInfo>();

            if (batchStrategy != null && !batchStrategy.isEmpty()) {
                // Pick latest run for each distinct pipeline using this strategy
                String sql = "SELECT r.* FROM run_metadata r " +
                    "INNER JOIN (SELECT pipeline_name, MAX(run_id) AS max_id " +
                    "FROM run_metadata WHERE batch_strategy = ? " +
                    "GROUP BY pipeline_name) latest " +
                    "ON r.run_id = latest.max_id ORDER BY r.pipeline_name";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, batchStrategy);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            runs.add(extractRunInfo(rs));
                        }
                    }
                }
            } else if (runIdsStr != null && !runIdsStr.isEmpty()) {
                // Parse comma-separated IDs
                String[] idParts = runIdsStr.split(",");
                for (String idStr : idParts) {
                    int rid = Integer.parseInt(idStr.trim());
                    String sql = "SELECT * FROM run_metadata WHERE run_id = ?";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, rid);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                runs.add(extractRunInfo(rs));
                            } else {
                                System.out.println("Warning: No run found with ID " + rid + ", skipping.");
                            }
                        }
                    }
                }
            }

            if (runs.size() < 2) {
                System.out.println("Error: Need at least 2 runs to compare. Found " + runs.size() + ".");
                return;
            }

            // Print comparison header
            System.out.println(LINE);
            System.out.println("PIPELINE COMPARISON");
            System.out.println(LINE);
            if (batchStrategy != null) {
                System.out.println("Batch Strategy: " + batchStrategy);
            }

            // Build column headers
            int labelWidth = 22;
            int colWidth = 14;
            StringBuilder header = new StringBuilder();
            header.append(padRight("", labelWidth));
            for (RunInfo r : runs) {
                header.append(padRight(r.pipeline, colWidth));
            }
            System.out.println(header.toString());
            System.out.println(repeat("-", labelWidth + colWidth * runs.size()));

            // Row: Run ID
            StringBuilder row = new StringBuilder();
            row.append(padRight("Run ID", labelWidth));
            for (RunInfo r : runs) {
                row.append(padRight(String.valueOf(r.runId), colWidth));
            }
            System.out.println(row.toString());

            // Row: Runtime
            row = new StringBuilder();
            row.append(padRight("Runtime (seconds)", labelWidth));
            for (RunInfo r : runs) {
                row.append(padRight(String.format("%.3f", r.runtime), colWidth));
            }
            System.out.println(row.toString());

            // Row: Total Records
            row = new StringBuilder();
            row.append(padRight("Total Records", labelWidth));
            for (RunInfo r : runs) {
                row.append(padRight(String.valueOf(r.totalRecords), colWidth));
            }
            System.out.println(row.toString());

            // Row: Malformed Records
            row = new StringBuilder();
            row.append(padRight("Malformed Records", labelWidth));
            for (RunInfo r : runs) {
                row.append(padRight(String.valueOf(r.totalMalformed), colWidth));
            }
            System.out.println(row.toString());

            // Row: Total Batches
            row = new StringBuilder();
            row.append(padRight("Total Batches", labelWidth));
            for (RunInfo r : runs) {
                row.append(padRight(String.valueOf(r.totalBatches), colWidth));
            }
            System.out.println(row.toString());

            // Row: Avg Batch Size
            row = new StringBuilder();
            row.append(padRight("Avg Batch Size", labelWidth));
            for (RunInfo r : runs) {
                row.append(padRight(String.format("%.2f", r.avgBatchSize), colWidth));
            }
            System.out.println(row.toString());

            System.out.println();

            // Runtime ranking
            List<RunInfo> sorted = new ArrayList<RunInfo>(runs);
            // Sort by runtime ascending (fastest first)
            for (int i = 0; i < sorted.size() - 1; i++) {
                for (int j = i + 1; j < sorted.size(); j++) {
                    if (sorted.get(j).runtime < sorted.get(i).runtime) {
                        RunInfo temp = sorted.get(i);
                        sorted.set(i, sorted.get(j));
                        sorted.set(j, temp);
                    }
                }
            }

            System.out.println("RUNTIME RANKING (fastest to slowest):");
            for (int i = 0; i < sorted.size(); i++) {
                RunInfo r = sorted.get(i);
                System.out.println("  " + (i + 1) + ". " +
                    padRight(r.pipeline, 14) + String.format("%.3f", r.runtime) + "s");
            }

            System.out.println();

            // Result consistency check
            System.out.println("RESULT CONSISTENCY CHECK:");
            checkQueryConsistency(conn, runs, "query1_daily_traffic", "Query 1 (Daily Traffic)");
            checkQueryConsistency(conn, runs, "query2_top_resources", "Query 2 (Top Resources)");
            checkQueryConsistency(conn, runs, "query3_hourly_errors", "Query 3 (Hourly Errors)");
        }
    }

    // =====================================================================
    // Private helper methods
    // =====================================================================

    private static void printBatchMetadata(Connection conn, int runId) throws SQLException {
        String sql = "SELECT * FROM batch_metadata WHERE run_id = ? ORDER BY batch_id";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                System.out.println("BATCH DETAILS");
                System.out.printf("%-10s %-12s %-12s %-12s %-15s%n",
                    "Batch ID", "Batch Size", "Malformed", "Valid", "Runtime (s)");
                System.out.println(repeat("-", 61));
                while (rs.next()) {
                    System.out.printf("%-10d %-12d %-12d %-12d %-15.3f%n",
                        rs.getInt("batch_id"),
                        rs.getInt("batch_size"),
                        rs.getInt("malformed_count"),
                        rs.getInt("valid_records"),
                        rs.getDouble("batch_runtime_seconds"));
                }
                System.out.println();
            }
        }
    }

    private static void printQueryResults(Connection conn, int runId,
            String tableName, String title, int displayLimit) throws SQLException {

        // First get total count
        int totalRows = 0;
        String countSql = "SELECT COUNT(*) FROM " + tableName + " WHERE run_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(countSql)) {
            ps.setInt(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    totalRows = rs.getInt(1);
                }
            }
        }

        if (totalRows == 0) {
            return; // No results for this query
        }

        // Then fetch rows with limit
        String sql = "SELECT * FROM " + tableName + " WHERE run_id = ? ORDER BY batch_id, id LIMIT " + displayLimit;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                int cols = meta.getColumnCount();

                System.out.println(title);
                // Print header
                StringBuilder header = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    header.append(String.format("%-20s", meta.getColumnName(i)));
                }
                System.out.println(header.toString());
                System.out.println(repeat("-", cols * 20));

                // Print rows
                int displayedRows = 0;
                while (rs.next()) {
                    StringBuilder rowStr = new StringBuilder();
                    for (int i = 1; i <= cols; i++) {
                        String val = rs.getString(i);
                        if (val == null) val = "NULL";
                        // Truncate long values for display
                        if (val.length() > 19) {
                            val = val.substring(0, 16) + "...";
                        }
                        rowStr.append(String.format("%-20s", val));
                    }
                    System.out.println(rowStr.toString());
                    displayedRows++;
                }

                // Show row count summary
                if (totalRows > displayedRows) {
                    System.out.println("(showing first " + displayedRows +
                        " rows -- " + totalRows + " total rows stored)");
                } else {
                    System.out.println("(" + totalRows + " total rows stored)");
                }
                System.out.println();
            }
        }
    }

    private static void checkQueryConsistency(Connection conn, List<RunInfo> runs,
            String tableName, String queryLabel) throws SQLException {
        // Compare row counts across all runs
        List<Integer> counts = new ArrayList<Integer>();
        for (RunInfo r : runs) {
            String sql = "SELECT COUNT(*) FROM " + tableName + " WHERE run_id = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, r.runId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        counts.add(rs.getInt(1));
                    } else {
                        counts.add(0);
                    }
                }
            }
        }

        // Check if all counts are the same
        boolean allSame = true;
        for (int i = 1; i < counts.size(); i++) {
            if (!counts.get(i).equals(counts.get(0))) {
                allSame = false;
                break;
            }
        }

        if (allSame) {
            System.out.println("  " + padRight(queryLabel + ":", 30) +
                "All pipelines produced identical row counts (" + counts.get(0) + " rows)");
        } else {
            StringBuilder detail = new StringBuilder();
            detail.append("  ").append(padRight(queryLabel + ":", 30))
                .append("Row counts differ: ");
            for (int i = 0; i < runs.size(); i++) {
                if (i > 0) detail.append(", ");
                detail.append(runs.get(i).pipeline).append("=").append(counts.get(i));
            }
            System.out.println(detail.toString());
        }
    }

    private static RunInfo extractRunInfo(ResultSet rs) throws SQLException {
        RunInfo info = new RunInfo();
        info.runId = rs.getInt("run_id");
        info.pipeline = rs.getString("pipeline_name");
        info.query = rs.getString("query_name");
        info.batchStrategy = rs.getString("batch_strategy");
        info.totalRecords = rs.getInt("total_records_processed");
        info.totalMalformed = rs.getInt("total_malformed_records");
        info.totalBatches = rs.getInt("total_batches");
        info.avgBatchSize = rs.getDouble("average_batch_size");
        info.runtime = rs.getDouble("runtime_seconds");
        return info;
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

    /**
     * Pads a string to the right with spaces to the given width.
     */
    private static String padRight(String s, int width) {
        if (s.length() >= width) return s;
        StringBuilder sb = new StringBuilder(s);
        for (int i = s.length(); i < width; i++) {
            sb.append(' ');
        }
        return sb.toString();
    }

    /**
     * Simple data holder for run metadata used in comparisons.
     */
    private static class RunInfo {
        int runId;
        String pipeline;
        String query;
        String batchStrategy;
        int totalRecords;
        int totalMalformed;
        int totalBatches;
        double avgBatchSize;
        double runtime;
    }
}
