package com.nasa.loganalytics.db;

import com.nasa.loganalytics.model.LogRecord;
import java.util.List;
import java.util.Map;

/**
 * Apache Pig pipeline — stub for Phase 2.
 * 
 * In Phase 2, this will execute ETL and aggregation logic through Pig Latin scripts.
 */
public class ApachePigDatabase extends Database {

    public ApachePigDatabase() {
        super("ApachePig");
    }

    @Override
    public void initialize() throws Exception {
        throw new UnsupportedOperationException(
            "Apache Pig pipeline is not yet implemented — scheduled for Phase 2.\n" +
            "Please use 'mapreduce' or 'mongodb' for now.");
    }

    @Override
    public void loadData(List<LogRecord> batch, int batchId) throws Exception {
        throw new UnsupportedOperationException("Apache Pig pipeline is not yet implemented — Phase 2");
    }

    @Override
    public List<Map<String, Object>> executeDailyTrafficSummary() throws Exception {
        throw new UnsupportedOperationException("Apache Pig pipeline is not yet implemented — Phase 2");
    }

    @Override
    public List<Map<String, Object>> executeTopRequestedResources() throws Exception {
        throw new UnsupportedOperationException("Apache Pig pipeline is not yet implemented — Phase 2");
    }

    @Override
    public List<Map<String, Object>> executeHourlyErrorAnalysis() throws Exception {
        throw new UnsupportedOperationException("Apache Pig pipeline is not yet implemented — Phase 2");
    }

    @Override
    public void cleanup() throws Exception {
        // No resources to clean
    }
}
