package com.etl.batching;

import com.etl.model.BatchData;

import java.util.List;

/**
 * Interface for batch splitting strategies.
 * Implementations split raw log files into discrete batches for pipeline processing.
 */
public interface BatchStrategy {
    /**
     * Splits the input log files into batches.
     * @param filePaths List of absolute paths to raw log files
     * @return List of BatchData objects (each containing batch_id and raw lines)
     */
    List<BatchData> split(List<String> filePaths);
}
