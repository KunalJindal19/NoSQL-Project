package com.etl.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.List;

/**
 * Loads ETL results and metadata into MySQL tables.
 */
public class ResultLoader {

    /**
     * Creates a new run_metadata record and returns the auto-generated run_id.
     */
    public static int createRunRecord(String pipeline, String query,
            String batchStrategy, int batchSize) throws SQLException {
        try (Connection conn = ConnectionManager.getConnection()) {
            String sql = "INSERT INTO run_metadata (pipeline_name, query_name, batch_strategy, " +
                "configured_batch_size) VALUES (?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sql,
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, pipeline);
                ps.setString(2, query);
                ps.setString(3, batchStrategy);
                if (batchSize > 0) {
                    ps.setInt(4, batchSize);
                } else {
                    ps.setNull(4, java.sql.Types.INTEGER);
                }
                ps.executeUpdate();
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    rs.next();
                    return rs.getInt(1);
                }
            }
        }
    }

    /**
     * Updates the run_metadata record with final totals after processing completes.
     */
    public static void updateRunRecord(int runId, int totalRecords, int totalMalformed,
            int totalBatches, double avgBatchSize, double runtime) throws SQLException {
        try (Connection conn = ConnectionManager.getConnection()) {
            String sql = "UPDATE run_metadata SET total_records_processed=?, " +
                "total_malformed_records=?, total_batches=?, average_batch_size=?, " +
                "runtime_seconds=? WHERE run_id=?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, totalRecords);
                ps.setInt(2, totalMalformed);
                ps.setInt(3, totalBatches);
                ps.setDouble(4, avgBatchSize);
                ps.setDouble(5, runtime);
                ps.setInt(6, runId);
                ps.executeUpdate();
            }
        }
    }

    /**
     * Dispatches query results to the appropriate table-specific loader.
     */
    public static void loadQueryResults(int runId, int batchId,
            String queryName, List<String[]> results) throws SQLException {
        try (Connection conn = ConnectionManager.getConnection()) {
            switch (queryName) {
                case "query1":
                    loadQuery1Results(conn, runId, batchId, results);
                    break;
                case "query2":
                    loadQuery2Results(conn, runId, batchId, results);
                    break;
                case "query3":
                    loadQuery3Results(conn, runId, batchId, results);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown query: " + queryName);
            }
        }
    }

    private static void loadQuery1Results(Connection conn, int runId, int batchId,
            List<String[]> results) throws SQLException {
        String sql = "INSERT INTO query1_daily_traffic " +
            "(run_id, batch_id, log_date, status_code, request_count, total_bytes) " +
            "VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String[] row : results) {
                ps.setInt(1, runId);
                ps.setInt(2, batchId);
                ps.setString(3, row[0]);                    // log_date
                ps.setInt(4, Integer.parseInt(row[1]));      // status_code
                ps.setInt(5, Integer.parseInt(row[2]));      // request_count
                ps.setLong(6, Long.parseLong(row[3]));       // total_bytes
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static void loadQuery2Results(Connection conn, int runId, int batchId,
            List<String[]> results) throws SQLException {
        String sql = "INSERT INTO query2_top_resources " +
            "(run_id, batch_id, resource_path, request_count, total_bytes, distinct_host_count) " +
            "VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String[] row : results) {
                ps.setInt(1, runId);
                ps.setInt(2, batchId);
                ps.setString(3, row[0]);                     // resource_path
                ps.setInt(4, Integer.parseInt(row[1]));       // request_count
                ps.setLong(5, Long.parseLong(row[2]));        // total_bytes
                ps.setInt(6, Integer.parseInt(row[3]));       // distinct_host_count
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static void loadQuery3Results(Connection conn, int runId, int batchId,
            List<String[]> results) throws SQLException {
        String sql = "INSERT INTO query3_hourly_errors " +
            "(run_id, batch_id, log_date, log_hour, error_request_count, total_request_count, " +
            "error_rate, distinct_error_hosts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String[] row : results) {
                ps.setInt(1, runId);
                ps.setInt(2, batchId);
                ps.setString(3, row[0]);                      // log_date
                ps.setString(4, row[1]);                      // log_hour
                ps.setInt(5, Integer.parseInt(row[2]));        // error_request_count
                ps.setInt(6, Integer.parseInt(row[3]));        // total_request_count
                ps.setDouble(7, Double.parseDouble(row[4]));   // error_rate
                ps.setInt(8, Integer.parseInt(row[5]));        // distinct_error_hosts
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /**
     * Stores batch-level metadata.
     */
    public static void storeBatchMetadata(int runId, int batchId,
            int batchSize, int malformedCount, int validRecords,
            double batchRuntime) throws SQLException {
        try (Connection conn = ConnectionManager.getConnection()) {
            String sql = "INSERT INTO batch_metadata " +
                "(run_id, batch_id, batch_size, malformed_count, valid_records, batch_runtime_seconds) " +
                "VALUES (?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, runId);
                ps.setInt(2, batchId);
                ps.setInt(3, batchSize);
                ps.setInt(4, malformedCount);
                ps.setInt(5, validRecords);
                ps.setDouble(6, batchRuntime);
                ps.executeUpdate();
            }
        }
    }

    /**
     * Stores malformed record summary for a batch.
     */
    public static void storeMalformedSummary(int runId, int batchId,
            int malformedCount) throws SQLException {
        try (Connection conn = ConnectionManager.getConnection()) {
            String sql = "INSERT INTO malformed_summary " +
                "(run_id, batch_id, malformed_count) VALUES (?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, runId);
                ps.setInt(2, batchId);
                ps.setInt(3, malformedCount);
                ps.executeUpdate();
            }
        }
    }
}
