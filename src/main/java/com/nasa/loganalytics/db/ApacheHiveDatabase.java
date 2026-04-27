package com.nasa.loganalytics.db;

import com.nasa.loganalytics.model.LogRecord;
import java.util.List;
import java.util.Map;

/**
 * Apache Hive pipeline — stub for Phase 2.
 * 
 * In Phase 2, this will execute ETL and aggregation logic through HiveQL queries.
 */
public class ApacheHiveDatabase extends Database {

    public ApacheHiveDatabase() {
        super("ApacheHive");
    }

    @Override
    public void initialize() throws Exception {
        throw new UnsupportedOperationException(
            "Apache Hive pipeline is not yet implemented — scheduled for Phase 2.\n" +
            "Please use 'mapreduce' or 'mongodb' for now.");
    }

    @Override
    public void loadData(List<LogRecord> batch, int batchId) throws Exception {
        throw new UnsupportedOperationException("Apache Hive pipeline is not yet implemented — Phase 2");
    }

    @Override
    public List<Map<String, Object>> executeDailyTrafficSummary() throws Exception {
        throw new UnsupportedOperationException("Apache Hive pipeline is not yet implemented — Phase 2");
    }

    @Override
    public List<Map<String, Object>> executeTopRequestedResources() throws Exception {
        throw new UnsupportedOperationException("Apache Hive pipeline is not yet implemented — Phase 2");
    }

    @Override
    public List<Map<String, Object>> executeHourlyErrorAnalysis() throws Exception {
        throw new UnsupportedOperationException("Apache Hive pipeline is not yet implemented — Phase 2");
    }

    @Override
    public void cleanup() throws Exception {
        // No resources to clean
    }
}
