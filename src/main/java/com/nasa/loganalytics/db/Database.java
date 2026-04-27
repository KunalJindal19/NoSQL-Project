package com.nasa.loganalytics.db;

import com.nasa.loganalytics.model.LogRecord;

import java.util.List;
import java.util.Map;

/**
 * Abstract base class for all pipeline implementations.
 * 
 * Each subclass represents a different NoSQL/Big Data execution backend
 * (MapReduce, MongoDB, Pig, Hive) but must implement equivalent semantics:
 * same parsing, cleaning, filtering, grouping, and aggregation logic.
 */
public abstract class Database {

    protected String pipelineName;
    protected int batchSize;
    protected String runId;
    protected int totalRecords;
    protected int numBatches;
    protected int malformedCount;

    public Database(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    // ======================== Lifecycle ========================

    /**
     * Initialize the pipeline backend (connections, configurations, etc.).
     */
    public abstract void initialize() throws Exception;

    /**
     * Load a batch of parsed log records into the pipeline's data store.
     * @param batch   List of parsed LogRecord objects
     * @param batchId Sequential batch identifier (starting from 1)
     */
    public abstract void loadData(List<LogRecord> batch, int batchId) throws Exception;

    /**
     * Clean up resources after processing is complete.
     */
    public abstract void cleanup() throws Exception;

    // ======================== Queries ========================

    /**
     * Query 1: Daily Traffic Summary
     * For each log date and status code, compute total requests and total bytes.
     * Output: log_date, status_code, request_count, total_bytes
     */
    public abstract List<Map<String, Object>> executeDailyTrafficSummary() throws Exception;

    /**
     * Query 2: Top 20 Requested Resources
     * Top 20 resource paths by request count with total bytes and distinct hosts.
     * Output: resource_path, request_count, total_bytes, distinct_host_count
     */
    public abstract List<Map<String, Object>> executeTopRequestedResources() throws Exception;

    /**
     * Query 3: Hourly Error Analysis
     * For each log date and hour, compute error counts (status 400-599),
     * total requests, error rate, and distinct error hosts.
     * Output: log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts
     */
    public abstract List<Map<String, Object>> executeHourlyErrorAnalysis() throws Exception;

    // ======================== Metadata ========================

    public String getPipelineName() {
        return pipelineName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public int getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(int totalRecords) {
        this.totalRecords = totalRecords;
    }

    public int getNumBatches() {
        return numBatches;
    }

    public void setNumBatches(int numBatches) {
        this.numBatches = numBatches;
    }

    public int getMalformedCount() {
        return malformedCount;
    }

    public void setMalformedCount(int malformedCount) {
        this.malformedCount = malformedCount;
    }

    public double getAvgBatchSize() {
        if (numBatches == 0) return 0;
        return (double) totalRecords / numBatches;
    }
}
