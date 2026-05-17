package com.etl.batching;

import com.etl.config.AppConfig;
import com.etl.model.BatchData;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * File-based batching: each raw log file is treated as one batch.
 * July log file -> Batch 1, August log file -> Batch 2.
 * No --batch-size argument needed.
 */
public class FileBasedBatchStrategy implements BatchStrategy {

    @Override
    public List<BatchData> split(List<String> filePaths) {
        List<BatchData> batches = new ArrayList<BatchData>();
        List<String> sortedPaths = new ArrayList<String>(filePaths);
        Collections.sort(sortedPaths); // Ensure consistent ordering
        int batchId = 1;

        for (String filePath : sortedPaths) {
            List<String> lines = new ArrayList<String>();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(new FileInputStream(filePath), AppConfig.LOG_FILE_ENCODING))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (!line.isEmpty()) {
                        lines.add(line);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading file: " + filePath, e);
            }
            batches.add(new BatchData(batchId++, lines));
        }
        return batches;
    }
}
