package com.etl.pipeline;

import com.etl.config.AppConfig;
import com.etl.model.PipelineResult;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Apache Pig pipeline implementation.
 * Invokes pig -x local via ProcessBuilder.
 */
public class PigPipeline implements Pipeline {

    @Override
    public PipelineResult execute(String batchFile, String queryName, int batchId) throws Exception {
        String scriptDir = AppConfig.PIG_SCRIPTS_DIR;
        String scriptPath = scriptDir + "/etl_" + queryName + ".pig";
        String outputDir = AppConfig.TEMP_OUTPUT_DIR + "/pig/" + queryName + "_batch_" + batchId;

        deleteDirectory(new File(outputDir));

        ProcessBuilder pb = new ProcessBuilder(
            "pig", "-x", "local",
            "-param", "INPUT=" + batchFile,
            "-param", "OUTPUT=" + outputDir,
            "-f", scriptPath
        );
        pb.environment().put("PIG_CLASSPATH",
            System.getenv("HADOOP_CONF_DIR") != null ? System.getenv("HADOOP_CONF_DIR") : "/opt/hadoop/etc/hadoop");
        pb.redirectErrorStream(true);

        Process process = pb.start();
        String output = readStream(process.getInputStream());
        int exitCode = process.waitFor();

        if (exitCode != 0) {
            System.err.println("[Pig] Script output:\n" + output);
            throw new RuntimeException("Pig failed for " + queryName + " (exit code " + exitCode + ")");
        }

        List<String[]> results = readTsvOutput(outputDir + "/results");
        int malformedCount = readMalformedCount(outputDir + "/malformed");
        return new PipelineResult(results, malformedCount);
    }

    private String readStream(InputStream is) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
        }
        return sb.toString();
    }

    private List<String[]> readTsvOutput(String outputDir) throws IOException {
        List<String[]> results = new ArrayList<String[]>();
        File dir = new File(outputDir);
        if (!dir.exists() || !dir.isDirectory()) return results;
        File[] files = dir.listFiles();
        if (files == null) return results;
        for (File file : files) {
            if (file.getName().startsWith("part-")) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (!line.isEmpty()) results.add(line.split("\t"));
                    }
                }
            }
        }
        return results;
    }

    private int readMalformedCount(String malformedDir) throws IOException {
        File dir = new File(malformedDir);
        if (!dir.exists() || !dir.isDirectory()) return 0;
        File[] files = dir.listFiles();
        if (files == null) return 0;
        for (File file : files) {
            if (file.getName().startsWith("part-")) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line = reader.readLine();
                    if (line != null && !line.trim().isEmpty()) return Integer.parseInt(line.trim());
                }
            }
        }
        return 0;
    }

    private void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) deleteDirectory(file);
                    else file.delete();
                }
            }
            dir.delete();
        }
    }
}
