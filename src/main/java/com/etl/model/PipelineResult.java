package com.etl.model;

import java.util.List;

/**
 * Represents the result of a pipeline execution for one query on one batch.
 * Contains the tabular query results and the count of malformed records.
 */
public class PipelineResult {
    private final List<String[]> results;
    private final int malformedCount;

    public PipelineResult(List<String[]> results, int malformedCount) {
        this.results = results;
        this.malformedCount = malformedCount;
    }

    public List<String[]> getResults() {
        return results;
    }

    public int getMalformedCount() {
        return malformedCount;
    }
}
