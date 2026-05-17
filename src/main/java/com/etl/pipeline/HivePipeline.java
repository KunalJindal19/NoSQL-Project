package com.etl.pipeline;

import com.etl.config.AppConfig;
import com.etl.model.PipelineResult;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Apache Hive pipeline implementation.
 * Invokes hive -f via ProcessBuilder with RegexSerDe for parsing.
 * Handles Derby lock cleanup before/after each invocation.
 */
public class HivePipeline implements Pipeline {

    @Override
    public PipelineResult execute(String batchFile, String queryName, int batchId) throws Exception {
        String scriptDir = AppConfig.HIVE_SCRIPTS_DIR;
        String outputDir = AppConfig.TEMP_OUTPUT_DIR + "/hive/" + queryName + "_batch_" + batchId;
        String batchDir = AppConfig.HIVE_BATCH_INPUT_DIR;

        // Prepare batch directory for Hive LOCATION
        File batchDirFile = new File(batchDir);
        batchDirFile.mkdirs();
        File[] existing = batchDirFile.listFiles();
        if (existing != null) {
            for (File f : existing) f.delete();
        }
        Files.copy(Paths.get(batchFile), Paths.get(batchDir, "data.log"),
                   StandardCopyOption.REPLACE_EXISTING);

        deleteDirectory(new File(outputDir));
        new File(outputDir).mkdirs();

        // Step 1: Create external table pointing to batch dir
        runHiveScript(scriptDir + "/create_table.hql",
            "--hivevar", "BATCH_DIR=" + batchDir);

        // Step 2: Run query
        runHiveScript(scriptDir + "/" + queryName + ".hql",
            "--hivevar", "OUTPUT_DIR=" + outputDir);

        // Read results
        List<String[]> results = readTsvOutput(outputDir + "/" + queryName);
        int malformedCount = readMalformedCount(outputDir + "/malformed");

        return new PipelineResult(results, malformedCount);
    }

    private void runHiveScript(String scriptPath, String... extraArgs) throws Exception {
        String hiveHome = System.getenv("HIVE_HOME") != null ? System.getenv("HIVE_HOME") : "/opt/hive";

        // Force cleanup of Derby locks before running
        new File(hiveHome + "/metastore_db/db.lck").delete();
        new File(hiveHome + "/metastore_db/dbex.lck").delete();

        try {
            List<String> cmd = new ArrayList<String>();
            cmd.add("hive");
            for (String arg : extraArgs) {
                cmd.add(arg);
            }
            cmd.add("-f");
            cmd.add(new File(scriptPath).getAbsolutePath());

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            // Set working directory to HIVE_HOME so Derby metastore is found
            pb.directory(new File(hiveHome));

            Process process = pb.start();
            String output = readStream(process.getInputStream());
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                System.err.println("[Hive] Script output:\n" + output);
                throw new RuntimeException("Hive failed (exit code " + exitCode + ")");
            }
        } finally {
            // Ensure locks are wiped even if the process throws an exception
            new File(hiveHome + "/metastore_db/db.lck").delete();
            new File(hiveHome + "/metastore_db/dbex.lck").delete();
        }
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
            // Hive output files are named 000000_0
            if (!file.isDirectory() && !file.getName().startsWith(".")) {
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
            if (!file.isDirectory() && !file.getName().startsWith(".")) {
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
