package com.etl.batching;

import com.etl.config.AppConfig;
import com.etl.model.BatchData;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;

/**
 * Writes batch data to temporary files for pipeline consumption.
 */
public class BatchWriter {

    /**
     * Writes a batch's lines to a temporary file.
     * @param batch The batch data to write
     * @param tempDir The directory to write the batch file into
     * @return The absolute path to the written batch file
     */
    public static String writeBatchToFile(BatchData batch, String tempDir) throws IOException {
        File dir = new File(tempDir);
        dir.mkdirs();
        String batchFilePath = tempDir + "/batch_" + batch.getBatchId() + ".log";
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(batchFilePath), AppConfig.LOG_FILE_ENCODING))) {
            for (String line : batch.getLines()) {
                writer.write(line);
                writer.newLine();
            }
        }
        return batchFilePath;
    }
}
