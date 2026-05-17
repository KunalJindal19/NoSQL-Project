package com.etl.config;

/**
 * Central configuration class for the ETL application.
 * Contains database credentials, file paths, and constants.
 *
 * *** IMPORTANT: Update MYSQL_PASSWORD below with your actual password ***
 */
public class AppConfig {

    // ============================================================
    // MySQL Configuration
    // >>> UPDATE THESE VALUES WITH YOUR ACTUAL CREDENTIALS <<< 
    // ============================================================
    public static final String MYSQL_URL = "jdbc:mysql://localhost:3306/etl_results";
    public static final String MYSQL_USER = "etl_user";   
    public static final String MYSQL_PASSWORD = "@anshKCS3105";  // <-- UPDATE THIS

    // ============================================================
    // File Paths
    // ============================================================
    public static final String DEFAULT_DATA_DIR = "./data";
    public static final String PIG_SCRIPTS_DIR = "scripts/pig";
    public static final String HIVE_SCRIPTS_DIR = "scripts/hive";

    // Temp directories for batch processing
    public static final String TEMP_BATCH_DIR = "/tmp/etl_batches";
    public static final String TEMP_OUTPUT_DIR = "/tmp/etl_output";
    public static final String HIVE_BATCH_INPUT_DIR = "/tmp/hive_batch_input";

    // ============================================================
    // MongoDB Configuration
    // ============================================================
    public static final String MONGO_CONNECTION_STRING = "mongodb://localhost:27017";
    public static final String MONGO_DATABASE = "etl_nasa";

    // ============================================================
    // File encoding for NASA log files
    // ============================================================
    public static final String LOG_FILE_ENCODING = "ISO-8859-1";

    // ============================================================
    // Log parsing regex (consistent across all pipelines)
    // ============================================================
    public static final String LOG_REGEX =
        "^(\\S+)\\s+-\\s+-\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+-\\d{4}\\]" +
        "\\s+\"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?\"\\s+(\\d{3})\\s+(\\S+)$";
}
