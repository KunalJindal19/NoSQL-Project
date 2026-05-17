package com.etl.model;

import java.util.List;

/**
 * Represents a single batch of raw log lines with a batch identifier.
 */
public class BatchData {
    private final int batchId;
    private final List<String> lines;

    public BatchData(int batchId, List<String> lines) {
        this.batchId = batchId;
        this.lines = lines;
    }

    public int getBatchId() {
        return batchId;
    }

    public List<String> getLines() {
        return lines;
    }
}
