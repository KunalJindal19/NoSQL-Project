package com.nasa.loganalytics.store;

import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * Handles persistence of query results and execution metadata to MySQL.
 * 
 * Uses 'one table per query' strategy as per assignment rules.
 */
public class MySQLStore {

    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String database;
    private Connection connection;

    public MySQLStore(String host, int port, String user, String password) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.database = "nasa_project";
    }

    /**
     * Connect to MySQL and ensure the schemas exist.
     */
    public void initialize() throws SQLException {
        String url = String.format("jdbc:mysql://%s:%d?useSSL=false&allowPublicKeyRetrieval=true", host, port);
        connection = DriverManager.getConnection(url, user, password);

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + database);
            stmt.executeUpdate("USE " + database);
            
            // Metadata table
            stmt.executeUpdate(
                "CREATE TABLE IF NOT EXISTS run_metadata (" +
                "  id              INT AUTO_INCREMENT PRIMARY KEY," +
                "  pipeline_name   VARCHAR(50)    NOT NULL," +
                "  run_id          VARCHAR(100)   NOT NULL," +
                "  query_name      VARCHAR(100)   NOT NULL," +
                "  execution_time  DOUBLE         NOT NULL," +
                "  batch_size      INT            NOT NULL," +
                "  total_records   INT            NOT NULL," +
                "  num_batches     INT            NOT NULL," +
                "  avg_batch_size  DOUBLE         NOT NULL," +
                "  malformed_count INT            NOT NULL DEFAULT 0," +
                "  executed_at     TIMESTAMP      DEFAULT CURRENT_TIMESTAMP," +
                "  INDEX idx_run (run_id)," +
                "  INDEX idx_query (query_name)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );

            // Query 1
            stmt.executeUpdate(
                "CREATE TABLE IF NOT EXISTS query1_results (" +
                "  id              INT AUTO_INCREMENT PRIMARY KEY," +
                "  pipeline_name   VARCHAR(50)    NOT NULL," +
                "  run_id          VARCHAR(100)   NOT NULL," +
                "  execution_time  DOUBLE         NOT NULL," +
                "  log_date        VARCHAR(20)    NOT NULL," +
                "  status_code     INT            NOT NULL," +
                "  request_count   BIGINT         NOT NULL," +
                "  total_bytes     BIGINT         NOT NULL," +
                "  INDEX idx_run (run_id)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );

            // Query 2
            stmt.executeUpdate(
                "CREATE TABLE IF NOT EXISTS query2_results (" +
                "  id                  INT AUTO_INCREMENT PRIMARY KEY," +
                "  pipeline_name       VARCHAR(50)    NOT NULL," +
                "  run_id              VARCHAR(100)   NOT NULL," +
                "  execution_time      DOUBLE         NOT NULL," +
                "  resource_path       VARCHAR(1000)  NOT NULL," +
                "  request_count       BIGINT         NOT NULL," +
                "  total_bytes         BIGINT         NOT NULL," +
                "  distinct_host_count BIGINT         NOT NULL," +
                "  INDEX idx_run (run_id)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );

            // Query 3
            stmt.executeUpdate(
                "CREATE TABLE IF NOT EXISTS query3_results (" +
                "  id                  INT AUTO_INCREMENT PRIMARY KEY," +
                "  pipeline_name       VARCHAR(50)    NOT NULL," +
                "  run_id              VARCHAR(100)   NOT NULL," +
                "  execution_time      DOUBLE         NOT NULL," +
                "  log_date            VARCHAR(20)    NOT NULL," +
                "  log_hour            INT            NOT NULL," +
                "  error_request_count BIGINT         NOT NULL," +
                "  total_request_count BIGINT         NOT NULL," +
                "  error_rate          DOUBLE         NOT NULL," +
                "  distinct_error_hosts BIGINT        NOT NULL," +
                "  INDEX idx_run (run_id)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );
        }
    }

    /**
     * Store query results with execution metadata into respective tables.
     */
    public void storeResults(String queryName, String pipelineName, String runId,
                             int batchId, double executionTime, int batchSize,
                             int totalRecords, int numBatches, double avgBatchSize,
                             int malformedCount, List<Map<String, Object>> results) throws SQLException {

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        
        try {
            // 1. Insert Metadata
            String metaSql = "INSERT INTO run_metadata " +
                             "(pipeline_name, run_id, query_name, execution_time, batch_size, " +
                             " total_records, num_batches, avg_batch_size, malformed_count) " +
                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(metaSql)) {
                pstmt.setString(1, pipelineName);
                pstmt.setString(2, runId);
                pstmt.setString(3, queryName);
                pstmt.setDouble(4, executionTime);
                pstmt.setInt(5, batchSize);
                pstmt.setInt(6, totalRecords);
                pstmt.setInt(7, numBatches);
                pstmt.setDouble(8, avgBatchSize);
                pstmt.setInt(9, malformedCount);
                pstmt.executeUpdate();
            }

            // 2. Insert Results using Batch API
            if (!results.isEmpty()) {
                switch (queryName) {
                    case "query1":
                        insertQuery1(pipelineName, runId, executionTime, results);
                        break;
                    case "query2":
                        insertQuery2(pipelineName, runId, executionTime, results);
                        break;
                    case "query3":
                        insertQuery3(pipelineName, runId, executionTime, results);
                        break;
                }
            }
            
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(autoCommit);
        }
    }

    private void insertQuery1(String pipelineName, String runId, double executionTime, 
                              List<Map<String, Object>> results) throws SQLException {
        String sql = "INSERT INTO query1_results (pipeline_name, run_id, execution_time, " +
                     "log_date, status_code, request_count, total_bytes) VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (Map<String, Object> row : results) {
                pstmt.setString(1, pipelineName);
                pstmt.setString(2, runId);
                pstmt.setDouble(3, executionTime);
                pstmt.setString(4, String.valueOf(row.get("log_date")));
                pstmt.setInt(5, ((Number) row.get("status_code")).intValue());
                pstmt.setLong(6, ((Number) row.get("request_count")).longValue());
                pstmt.setLong(7, ((Number) row.get("total_bytes")).longValue());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    private void insertQuery2(String pipelineName, String runId, double executionTime, 
                              List<Map<String, Object>> results) throws SQLException {
        String sql = "INSERT INTO query2_results (pipeline_name, run_id, execution_time, " +
                     "resource_path, request_count, total_bytes, distinct_host_count) VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (Map<String, Object> row : results) {
                pstmt.setString(1, pipelineName);
                pstmt.setString(2, runId);
                pstmt.setDouble(3, executionTime);
                pstmt.setString(4, String.valueOf(row.get("resource_path")));
                pstmt.setLong(5, ((Number) row.get("request_count")).longValue());
                pstmt.setLong(6, ((Number) row.get("total_bytes")).longValue());
                pstmt.setLong(7, ((Number) row.get("distinct_host_count")).longValue());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    private void insertQuery3(String pipelineName, String runId, double executionTime, 
                              List<Map<String, Object>> results) throws SQLException {
        String sql = "INSERT INTO query3_results (pipeline_name, run_id, execution_time, " +
                     "log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (Map<String, Object> row : results) {
                pstmt.setString(1, pipelineName);
                pstmt.setString(2, runId);
                pstmt.setDouble(3, executionTime);
                pstmt.setString(4, String.valueOf(row.get("log_date")));
                pstmt.setInt(5, ((Number) row.get("log_hour")).intValue());
                pstmt.setLong(6, ((Number) row.get("error_request_count")).longValue());
                pstmt.setLong(7, ((Number) row.get("total_request_count")).longValue());
                pstmt.setDouble(8, ((Number) row.get("error_rate")).doubleValue());
                pstmt.setLong(9, ((Number) row.get("distinct_error_hosts")).longValue());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    /**
     * Retrieve recent query runs for reporting (pulls from run_metadata).
     */
    public List<Map<String, Object>> getRecentResults(int limit) throws SQLException {
        List<Map<String, Object>> results = new java.util.ArrayList<>();
        String sql = "SELECT * FROM run_metadata ORDER BY executed_at DESC LIMIT ?";

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
     * Get runs for a specific run_id (pulls from run_metadata).
     */
    public List<Map<String, Object>> getResultsByRunId(String runId) throws SQLException {
        List<Map<String, Object>> results = new java.util.ArrayList<>();
        String sql = "SELECT * FROM run_metadata WHERE run_id = ? ORDER BY query_name";

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

    /**
     * Export all MySQL tables to a neatly formatted TSV text file.
     */
    public void exportAllResultsToFile(String filePath) throws SQLException {
        try (java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.FileWriter(filePath))) {
            writer.println("=========================================");
            writer.println("RUN METADATA");
            writer.println("=========================================");
            exportTableByPipeline(writer, "run_metadata");
            
            writer.println("\n=========================================");
            writer.println("QUERY 1 RESULTS (Daily Traffic Summary)");
            writer.println("=========================================");
            exportTableByPipeline(writer, "query1_results");
            
            writer.println("\n=========================================");
            writer.println("QUERY 2 RESULTS (Top 20 Resources)");
            writer.println("=========================================");
            exportTableByPipeline(writer, "query2_results");
            
            writer.println("\n=========================================");
            writer.println("QUERY 3 RESULTS (Hourly Error Analysis)");
            writer.println("=========================================");
            exportTableByPipeline(writer, "query3_results");
        } catch (java.io.IOException e) {
            throw new SQLException("Failed to write output file: " + filePath, e);
        }
    }

    private void exportTableByPipeline(java.io.PrintWriter writer, String tableName) throws SQLException {
        String[] pipelines = {"MongoDB", "MapReduce", "ApachePig", "ApacheHive"};
        for (String pipeline : pipelines) {
            writer.println("--- " + pipeline + " ---");
            String sql = "SELECT * FROM " + tableName + " WHERE pipeline_name = ?";
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, pipeline);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.isBeforeFirst()) {
                        writer.println("N/A\n");
                        continue;
                    }
                    
                    ResultSetMetaData meta = rs.getMetaData();
                    int cols = meta.getColumnCount();
                    
                    List<String[]> rows = new java.util.ArrayList<>();
                    String[] headers = new String[cols];
                    int[] colWidths = new int[cols];
                    
                    // Header row
                    for (int i = 0; i < cols; i++) {
                        headers[i] = meta.getColumnName(i + 1);
                        colWidths[i] = headers[i].length();
                    }
                    rows.add(headers);
                    
                    // Data rows
                    while (rs.next()) {
                        String[] row = new String[cols];
                        for (int i = 0; i < cols; i++) {
                            Object val = rs.getObject(i + 1);
                            row[i] = val != null ? val.toString() : "NULL";
                            colWidths[i] = Math.max(colWidths[i], row[i].length());
                        }
                        rows.add(row);
                    }
                    
                    // Print table with padding
                    for (int r = 0; r < rows.size(); r++) {
                        String[] row = rows.get(r);
                        for (int i = 0; i < cols; i++) {
                            writer.print(String.format("%-" + (colWidths[i] + 3) + "s", row[i]));
                        }
                        writer.println();
                        
                        // Separator line below headers
                        if (r == 0) {
                            for (int i = 0; i < cols; i++) {
                                for (int w = 0; w < colWidths[i] + 3; w++) {
                                    writer.print("-");
                                }
                            }
                            writer.println();
                        }
                    }
                    writer.println();
                }
            } catch (SQLException e) {
                writer.println("N/A (" + e.getMessage() + ")\n");
            }
        }
    }

    public void close() {
        if (connection != null) {
            try { connection.close(); } catch (SQLException ignored) {}
        }
    }
}
