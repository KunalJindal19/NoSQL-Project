package com.etl.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Scanner;

/**
 * Creates MySQL tables if they do not already exist.
 * Supports verbose per-table output and --reset mode.
 */
public class SchemaInitializer {

    private static final String[] TABLE_NAMES = {
        "run_metadata",
        "batch_metadata",
        "query1_daily_traffic",
        "query2_top_resources",
        "query3_hourly_errors",
        "malformed_summary"
    };

    // Drop order: child tables first to avoid FK constraint violations
    private static final String[] DROP_ORDER = {
        "malformed_summary",
        "query3_hourly_errors",
        "query2_top_resources",
        "query1_daily_traffic",
        "batch_metadata",
        "run_metadata"
    };

    private static final String[] CREATE_STATEMENTS = {
        // Table 1: Run-level metadata
        "CREATE TABLE IF NOT EXISTS run_metadata (" +
        "    run_id              INT AUTO_INCREMENT PRIMARY KEY," +
        "    pipeline_name       VARCHAR(20) NOT NULL," +
        "    query_name          VARCHAR(20) NOT NULL," +
        "    batch_strategy      VARCHAR(20) NOT NULL," +
        "    configured_batch_size INT," +
        "    total_records_processed INT NOT NULL DEFAULT 0," +
        "    total_malformed_records INT NOT NULL DEFAULT 0," +
        "    total_batches       INT NOT NULL DEFAULT 0," +
        "    average_batch_size  DECIMAL(12,2) NOT NULL DEFAULT 0," +
        "    runtime_seconds     DECIMAL(10,3) NOT NULL DEFAULT 0," +
        "    execution_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP" +
        ")",

        // Table 2: Batch-level metadata
        "CREATE TABLE IF NOT EXISTS batch_metadata (" +
        "    id                  INT AUTO_INCREMENT PRIMARY KEY," +
        "    run_id              INT NOT NULL," +
        "    batch_id            INT NOT NULL," +
        "    batch_size          INT NOT NULL," +
        "    malformed_count     INT NOT NULL DEFAULT 0," +
        "    valid_records       INT NOT NULL," +
        "    batch_runtime_seconds DECIMAL(10,3)," +
        "    FOREIGN KEY (run_id) REFERENCES run_metadata(run_id) ON DELETE CASCADE" +
        ")",

        // Table 3: Query 1 results - Daily Traffic Summary
        "CREATE TABLE IF NOT EXISTS query1_daily_traffic (" +
        "    id                  INT AUTO_INCREMENT PRIMARY KEY," +
        "    run_id              INT NOT NULL," +
        "    batch_id            INT NOT NULL," +
        "    log_date            VARCHAR(20) NOT NULL," +
        "    status_code         INT NOT NULL," +
        "    request_count       INT NOT NULL," +
        "    total_bytes         BIGINT NOT NULL," +
        "    FOREIGN KEY (run_id) REFERENCES run_metadata(run_id) ON DELETE CASCADE" +
        ")",

        // Table 4: Query 2 results - Top Requested Resources
        "CREATE TABLE IF NOT EXISTS query2_top_resources (" +
        "    id                  INT AUTO_INCREMENT PRIMARY KEY," +
        "    run_id              INT NOT NULL," +
        "    batch_id            INT NOT NULL," +
        "    resource_path       VARCHAR(2048) NOT NULL," +
        "    request_count       INT NOT NULL," +
        "    total_bytes         BIGINT NOT NULL," +
        "    distinct_host_count INT NOT NULL," +
        "    FOREIGN KEY (run_id) REFERENCES run_metadata(run_id) ON DELETE CASCADE" +
        ")",

        // Table 5: Query 3 results - Hourly Error Analysis
        "CREATE TABLE IF NOT EXISTS query3_hourly_errors (" +
        "    id                  INT AUTO_INCREMENT PRIMARY KEY," +
        "    run_id              INT NOT NULL," +
        "    batch_id            INT NOT NULL," +
        "    log_date            VARCHAR(20) NOT NULL," +
        "    log_hour            VARCHAR(5) NOT NULL," +
        "    error_request_count INT NOT NULL," +
        "    total_request_count INT NOT NULL," +
        "    error_rate          DECIMAL(8,6) NOT NULL," +
        "    distinct_error_hosts INT NOT NULL," +
        "    FOREIGN KEY (run_id) REFERENCES run_metadata(run_id) ON DELETE CASCADE" +
        ")",

        // Table 6: Malformed record summary per batch
        "CREATE TABLE IF NOT EXISTS malformed_summary (" +
        "    id                  INT AUTO_INCREMENT PRIMARY KEY," +
        "    run_id              INT NOT NULL," +
        "    batch_id            INT NOT NULL," +
        "    malformed_count     INT NOT NULL," +
        "    sample_lines        TEXT," +
        "    FOREIGN KEY (run_id) REFERENCES run_metadata(run_id) ON DELETE CASCADE" +
        ")"
    };

    /**
     * Creates all tables with verbose per-table output.
     * Uses CREATE TABLE IF NOT EXISTS (safe, preserves data).
     */
    public static void initialize() throws SQLException {
        System.out.println("Connecting to MySQL at localhost:3306/etl_results...");
        try (Connection conn = ConnectionManager.getConnection();
             Statement stmt = conn.createStatement()) {

            DatabaseMetaData dbMeta = conn.getMetaData();

            for (int i = 0; i < TABLE_NAMES.length; i++) {
                String tableName = TABLE_NAMES[i];
                boolean existed = tableExists(dbMeta, tableName);

                stmt.executeUpdate(CREATE_STATEMENTS[i]);

                String status = existed ? "already exists" : "created";
                System.out.printf("Creating table: %-25s ... OK (%s)%n", tableName, status);
            }

            System.out.println();
            System.out.println("Schema initialization complete. " + TABLE_NAMES.length + " tables ready.");
        }
    }

    /**
     * Drops all existing tables and recreates them.
     * Requires user confirmation via stdin.
     */
    public static void reset() throws SQLException {
        System.out.println("WARNING: This will delete ALL existing run data.");
        System.out.print("Type 'yes' to confirm: ");
        System.out.flush();

        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine().trim();
        if (!"yes".equals(input)) {
            System.out.println("Aborted. No changes made.");
            return;
        }

        System.out.println();
        System.out.println("Connecting to MySQL at localhost:3306/etl_results...");
        System.out.println("Dropping existing tables...");

        try (Connection conn = ConnectionManager.getConnection();
             Statement stmt = conn.createStatement()) {

            // Drop in reverse dependency order (children first)
            for (String tableName : DROP_ORDER) {
                stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
            }

            // Recreate all tables
            for (int i = 0; i < TABLE_NAMES.length; i++) {
                stmt.executeUpdate(CREATE_STATEMENTS[i]);
                System.out.printf("Creating table: %-25s ... OK (created)%n", TABLE_NAMES[i]);
            }

            System.out.println();
            System.out.println("Schema reset complete. " + TABLE_NAMES.length +
                " tables ready. All previous data has been deleted.");
        }
    }

    /**
     * Checks whether a table already exists in the database.
     */
    private static boolean tableExists(DatabaseMetaData dbMeta, String tableName) throws SQLException {
        try (ResultSet rs = dbMeta.getTables(null, null, tableName, null)) {
            return rs.next();
        }
    }
}
