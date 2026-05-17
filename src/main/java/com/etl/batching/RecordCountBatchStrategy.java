package com.etl.batching;

import com.etl.config.AppConfig;
import com.etl.model.BatchData;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Splits log files into fixed-size record-count batches.
 * Every N lines are grouped into one batch, with sequential batch IDs starting from 1.
 */
public class RecordCountBatchStrategy implements BatchStrategy {
    private final int batchSize;

    public RecordCountBatchStrategy(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public List<BatchData> split(List<String> filePaths) {
        List<BatchData> batches = new ArrayList<BatchData>();
        int batchId = 1;
        List<String> currentBatch = new ArrayList<String>();

        for (String filePath : filePaths) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(new FileInputStream(filePath), AppConfig.LOG_FILE_ENCODING))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (!line.isEmpty()) {
                        currentBatch.add(line);
                        if (currentBatch.size() == batchSize) {
                            batches.add(new BatchData(batchId++, new ArrayList<String>(currentBatch)));
                            currentBatch.clear();
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading file: " + filePath, e);
            }
        }
        if (!currentBatch.isEmpty()) {
            batches.add(new BatchData(batchId, new ArrayList<String>(currentBatch)));
        }
        return batches;
    }
}
