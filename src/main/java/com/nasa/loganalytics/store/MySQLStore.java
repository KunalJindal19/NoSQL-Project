package com.nasa.loganalytics.store;

import com.google.gson.Gson;

import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * Handles persistence of query results and execution metadata to MySQL.
 * 
 * Uses a single `query_results` table with a JSON column for result data,
 * plus metadata columns for pipeline name, run ID, timing, and batch stats.
 */
public class MySQLStore {

    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String database;
    private Connection connection;
    private final Gson gson = new Gson();

    public MySQLStore(String host, int port, String user, String password) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.database = "nasa_project";
    }

    /**
     * Connect to MySQL and ensure the schema exists.
     */
    public void initialize() throws SQLException {
        // Connect without specifying database first
        String url = String.format("jdbc:mysql://%s:%d?useSSL=false&allowPublicKeyRetrieval=true", host, port);
        connection = DriverManager.getConnection(url, user, password);

        // Create database if not exists
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + database);
            stmt.executeUpdate("USE " + database);
        }

        // Create results table
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(
                "CREATE TABLE IF NOT EXISTS query_results (" +
                "  id              INT AUTO_INCREMENT PRIMARY KEY," +
                "  pipeline_name   VARCHAR(50)    NOT NULL," +
                "  run_id          VARCHAR(100)   NOT NULL," +
                "  batch_id        INT            NOT NULL," +
                "  query_name      VARCHAR(100)   NOT NULL," +
                "  result_json     JSON           NOT NULL," +
                "  execution_time  DOUBLE         NOT NULL," +
                "  batch_size      INT            NOT NULL," +
                "  total_records   INT            NOT NULL," +
                "  num_batches     INT            NOT NULL," +
                "  avg_batch_size  DOUBLE         NOT NULL," +
                "  malformed_count INT            NOT NULL DEFAULT 0," +
                "  executed_at     TIMESTAMP      DEFAULT CURRENT_TIMESTAMP," +
                "  INDEX idx_pipeline (pipeline_name)," +
                "  INDEX idx_run (run_id)," +
                "  INDEX idx_query (query_name)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );
        }
    }

    /**
     * Store query results with execution metadata.
     */
    public void storeResults(String queryName, String pipelineName, String runId,
                             int batchId, double executionTime, int batchSize,
                             int totalRecords, int numBatches, double avgBatchSize,
                             int malformedCount, List<Map<String, Object>> results) throws SQLException {

        String sql = "INSERT INTO query_results " +
                     "(pipeline_name, run_id, batch_id, query_name, result_json, " +
                     " execution_time, batch_size, total_records, num_batches, avg_batch_size, malformed_count) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, pipelineName);
            pstmt.setString(2, runId);
            pstmt.setInt(3, batchId);
            pstmt.setString(4, queryName);
            pstmt.setString(5, gson.toJson(results));
            pstmt.setDouble(6, executionTime);
            pstmt.setInt(7, batchSize);
            pstmt.setInt(8, totalRecords);
            pstmt.setInt(9, numBatches);
            pstmt.setDouble(10, avgBatchSize);
            pstmt.setInt(11, malformedCount);
            pstmt.executeUpdate();
        }
    }

    /**
     * Retrieve recent query results for reporting.
     */
    public List<Map<String, Object>> getRecentResults(int limit) throws SQLException {
        List<Map<String, Object>> results = new java.util.ArrayList<>();
        String sql = "SELECT * FROM query_results ORDER BY executed_at DESC LIMIT ?";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setInt(1, limit);
            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                int cols = meta.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new java.util.LinkedHashMap<>();
                    for (int i = 1; i <= cols; i++) {
                        row.put(meta.getColumnName(i), rs.getObject(i));
                    }
                    results.add(row);
                }
            }
        }
        return results;
    }

    /**
     * Get results for a specific run.
     */
    public List<Map<String, Object>> getResultsByRunId(String runId) throws SQLException {
        List<Map<String, Object>> results = new java.util.ArrayList<>();
        String sql = "SELECT * FROM query_results WHERE run_id = ? ORDER BY query_name";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, runId);
            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                int cols = meta.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new java.util.LinkedHashMap<>();
                    for (int i = 1; i <= cols; i++) {
                        row.put(meta.getColumnName(i), rs.getObject(i));
                    }
                    results.add(row);
                }
            }
        }
        return results;
    }

    public void close() {
        if (connection != null) {
            try { connection.close(); } catch (SQLException ignored) {}
        }
    }
}
