package com.etl.pipeline;

import com.etl.model.PipelineResult;

/**
 * Interface for all ETL pipeline implementations.
 * Each pipeline handles: data loading, parsing, cleaning, and query execution.
 */
public interface Pipeline {
    /**
     * Executes a query on a single batch file.
     * @param batchFile Absolute path to the batch file containing raw log lines
     * @param queryName One of "query1", "query2", "query3"
     * @param batchId The batch identifier
     * @return PipelineResult containing query output rows and malformed record count
     * @throws Exception if pipeline execution fails
     */
    PipelineResult execute(String batchFile, String queryName, int batchId) throws Exception;
}
