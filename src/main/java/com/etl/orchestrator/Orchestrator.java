package com.etl.orchestrator;

import com.etl.batching.BatchStrategy;
import com.etl.batching.BatchWriter;
import com.etl.batching.FileBasedBatchStrategy;
import com.etl.batching.RecordCountBatchStrategy;
import com.etl.config.AppConfig;
import com.etl.db.ResultLoader;
import com.etl.model.BatchData;
import com.etl.model.PipelineResult;
import com.etl.pipeline.HivePipeline;
import com.etl.pipeline.MapReducePipeline;
import com.etl.pipeline.MongoDBPipeline;
import com.etl.pipeline.PigPipeline;
import com.etl.pipeline.Pipeline;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Central orchestrator that coordinates:
 * - Batch splitting
 * - Pipeline invocation for each batch
 * - MySQL result loading
 * - Runtime measurement
 */
public class Orchestrator {

    public int executeRun(String pipelineName, String queryName,
                          String batchStrategy, int batchSize, String dataDir) throws Exception {
        // Get file paths
        List<String> logFiles = getLogFiles(dataDir);

        // Print file info
        StringBuilder fileNames = new StringBuilder();
        for (int i = 0; i < logFiles.size(); i++) {
            if (i > 0) fileNames.append(", ");
            fileNames.append(new File(logFiles.get(i)).getName());
        }
        System.out.println("  Found " + logFiles.size() + " log files: " + fileNames.toString());

        // Split into batches based on strategy
        BatchStrategy strategy;
        switch (batchStrategy) {
            case "record_count":
                strategy = new RecordCountBatchStrategy(batchSize);
                System.out.println("  Batch strategy: record_count (" + batchSize + " records per batch)");
                break;
            case "file_based":
                strategy = new FileBasedBatchStrategy();
                System.out.println("  Batch strategy: file_based (one batch per input file)");
                break;
            default:
                throw new IllegalArgumentException("Unknown batch strategy: " + batchStrategy);
        }

        List<BatchData> batches = strategy.split(logFiles);

        // Count total lines
        int totalLinesRead = 0;
        for (BatchData b : batches) {
            totalLinesRead += b.getLines().size();
        }
        System.out.println("  Total lines read: " + totalLinesRead);
        System.out.println("  Created " + batches.size() + " batches");

        // Create run_metadata record in MySQL (get run_id)
        int runId = ResultLoader.createRunRecord(
            pipelineName, queryName, batchStrategy, batchSize);
        System.out.println();
        System.out.println("Initializing run... Run ID: " + runId);

        // Select pipeline implementation
        Pipeline pipeline;
        switch (pipelineName) {
            case "pig":       pipeline = new PigPipeline(); break;
            case "mapreduce": pipeline = new MapReducePipeline(); break;
            case "mongodb":   pipeline = new MongoDBPipeline(); break;
            case "hive":      pipeline = new HivePipeline(); break;
            default: throw new IllegalArgumentException("Unknown pipeline: " + pipelineName);
        }

        // Determine which queries to run
        List<String> queriesToRun;
        if ("all".equals(queryName)) {
            queriesToRun = Arrays.asList("query1", "query2", "query3");
        } else {
            queriesToRun = Collections.singletonList(queryName);
        }

        System.out.println();
        System.out.println("Processing batches...");

        // START TIMING HERE
        long startTime = System.nanoTime();

        int totalRecords = 0;
        int totalMalformed = 0;
        int totalBatches = 0;
        int numBatches = batches.size();

        for (BatchData batch : batches) {
            totalBatches++;
            int batchRecordCount = batch.getLines().size();
            totalRecords += batchRecordCount;

            // Write batch to temp file
            String batchFile = BatchWriter.writeBatchToFile(batch, AppConfig.TEMP_BATCH_DIR);

            long batchStart = System.nanoTime();
            int batchMalformed = 0;

            // Build query status string as queries complete
            StringBuilder queryStatus = new StringBuilder();
            for (String qname : queriesToRun) {
                // Invoke the selected pipeline
                PipelineResult result = pipeline.execute(batchFile, qname, batch.getBatchId());

                // Load query results into MySQL
                ResultLoader.loadQueryResults(runId, batch.getBatchId(), qname, result.getResults());

                batchMalformed = result.getMalformedCount();

                // Append checkmark for this query
                if (queryStatus.length() > 0) queryStatus.append(" ");
                queryStatus.append(qname).append(" done");
            }

            long batchEnd = System.nanoTime();
            double batchRuntime = (batchEnd - batchStart) / 1_000_000_000.0;

            totalMalformed += batchMalformed;

            // Store batch metadata
            int validRecords = batchRecordCount - batchMalformed;
            ResultLoader.storeBatchMetadata(runId, batch.getBatchId(),
                batchRecordCount, batchMalformed, validRecords, batchRuntime);

            // Store malformed summary
            ResultLoader.storeMalformedSummary(runId, batch.getBatchId(), batchMalformed);

            // Clean up temp files
            new File(batchFile).delete();

            // Print single-line batch progress
            System.out.println("  Batch " + batch.getBatchId() + "/" + numBatches +
                ": " + batchRecordCount + " records | Malformed: " + batchMalformed +
                " | " + queryStatus.toString() + " | " +
                String.format("%.3f", batchRuntime) + "s");
            System.out.flush();
        }

        // STOP TIMING HERE
        long endTime = System.nanoTime();
        double runtime = (endTime - startTime) / 1_000_000_000.0;

        System.out.println();
        System.out.println("All batches processed. Loading final metadata to MySQL...");

        // Update run_metadata with final totals
        double avgBatchSize = totalBatches > 0 ? (double) totalRecords / totalBatches : 0;
        ResultLoader.updateRunRecord(runId, totalRecords, totalMalformed,
            totalBatches, avgBatchSize, runtime);

        return runId;
    }

    /**
     * Finds all log files in the specified data directory.
     * Looks for files matching NASA_access_log_* pattern.
     */
    private List<String> getLogFiles(String dataDir) {
        File dir = new File(dataDir);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new RuntimeException("Data directory does not exist: " + dir.getAbsolutePath());
        }

        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            throw new RuntimeException("No files found in data directory: " + dir.getAbsolutePath());
        }

        List<String> logFiles = new ArrayList<String>();
        for (File file : files) {
            if (file.isFile() && file.getName().startsWith("NASA_access_log")) {
                logFiles.add(file.getAbsolutePath());
            }
        }

        if (logFiles.isEmpty()) {
            throw new RuntimeException("No NASA log files found in: " + dir.getAbsolutePath());
        }

        Collections.sort(logFiles);
        return logFiles;
    }
}
