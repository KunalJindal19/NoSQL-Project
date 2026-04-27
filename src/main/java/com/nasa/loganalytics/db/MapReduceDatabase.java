package com.nasa.loganalytics.db;

import com.nasa.loganalytics.model.LogRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.*;

/**
 * Hadoop MapReduce pipeline implementation.
 * 
 * Runs in Hadoop local (standalone) mode — no HDFS or YARN required.
 * Each query is a separate MapReduce job that reads from the loaded data file.
 * 
 * Data flow:
 *   loadData() → writes parsed records to a temp TSV file
 *   executeQuery*() → runs MR job on that file → reads output
 */
public class MapReduceDatabase extends Database {

    private Configuration hadoopConf;
    private File dataDir;
    private File inputFile;
    private File outputDir;

    public MapReduceDatabase() {
        super("MapReduce");
    }

    @Override
    public void initialize() throws Exception {
        hadoopConf = new Configuration();
        hadoopConf.set("mapreduce.framework.name", "local");
        hadoopConf.set("fs.defaultFS", "file:///");

        // Create temp directories
        dataDir = Files.createTempDirectory("nasa_mr_").toFile();
        inputFile = new File(dataDir, "input.tsv");
        outputDir = new File(dataDir, "output");

        // Ensure clean state
        if (inputFile.exists()) inputFile.delete();
    }

    /**
     * Load data by appending parsed records to a TSV file.
     * Format: host \t logDate \t logHour \t httpMethod \t resourcePath \t protocolVersion \t statusCode \t bytesTransferred
     */
    @Override
    public void loadData(List<LogRecord> batch, int batchId) throws Exception {
        try (PrintWriter writer = new PrintWriter(new FileWriter(inputFile, true))) {
            for (LogRecord record : batch) {
                if (record.isMalformed()) continue;

                writer.printf("%s\t%s\t%d\t%s\t%s\t%s\t%d\t%d%n",
                        record.getHost(),
                        record.getLogDate(),
                        record.getLogHour(),
                        record.getHttpMethod(),
                        record.getResourcePath(),
                        record.getProtocolVersion(),
                        record.getStatusCode(),
                        record.getBytesTransferred());
            }
        }
    }

    // ======================== Query 1: Daily Traffic Summary ========================

    /**
     * Query 1: Daily Traffic Summary
     * For each (logDate, statusCode) → count requests, sum bytes
     */
    @Override
    public List<Map<String, Object>> executeDailyTrafficSummary() throws Exception {
        File queryOutput = new File(outputDir, "query1");
        if (queryOutput.exists()) deleteDir(queryOutput);

        Job job = Job.getInstance(hadoopConf, "Query1_DailyTrafficSummary");
        job.setJarByClass(MapReduceDatabase.class);

        job.setMapperClass(Query1Mapper.class);
        job.setCombinerClass(Query1Reducer.class);
        job.setReducerClass(Query1Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputFile.getAbsolutePath()));
        FileOutputFormat.setOutputPath(job, new Path(queryOutput.getAbsolutePath()));

        if (!job.waitForCompletion(false)) {
            throw new RuntimeException("MapReduce Query 1 failed");
        }

        return readQuery1Output(queryOutput);
    }

    public static class Query1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 8) return;

            String logDate = fields[1];
            String statusCode = fields[6];
            String bytes = fields[7];

            outKey.set(logDate + "\t" + statusCode);
            outVal.set("1\t" + bytes);
            context.write(outKey, outVal);
        }
    }

    public static class Query1Reducer extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long requestCount = 0;
            long totalBytes = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                requestCount += Long.parseLong(parts[0]);
                totalBytes += Long.parseLong(parts[1]);
            }

            outVal.set(requestCount + "\t" + totalBytes);
            context.write(key, outVal);
        }
    }

    private List<Map<String, Object>> readQuery1Output(File outputDir) throws IOException {
        List<Map<String, Object>> results = new ArrayList<>();
        for (File file : outputDir.listFiles()) {
            if (file.getName().startsWith("part-")) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length >= 4) {
                            Map<String, Object> row = new LinkedHashMap<>();
                            row.put("log_date", parts[0]);
                            row.put("status_code", Integer.parseInt(parts[1]));
                            row.put("request_count", Long.parseLong(parts[2]));
                            row.put("total_bytes", Long.parseLong(parts[3]));
                            results.add(row);
                        }
                    }
                }
            }
        }
        results.sort(Comparator.comparing((Map<String, Object> m) -> (String) m.get("log_date"))
                .thenComparing(m -> (Integer) m.get("status_code")));
        return results;
    }

    // ======================== Query 2: Top 20 Requested Resources ========================

    /**
     * Query 2: Top 20 Requested Resources
     * Two-pass: first aggregate per resource, then sort + limit
     */
    @Override
    public List<Map<String, Object>> executeTopRequestedResources() throws Exception {
        File queryOutput = new File(outputDir, "query2");
        if (queryOutput.exists()) deleteDir(queryOutput);

        Job job = Job.getInstance(hadoopConf, "Query2_TopRequestedResources");
        job.setJarByClass(MapReduceDatabase.class);

        job.setMapperClass(Query2Mapper.class);
        job.setReducerClass(Query2Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputFile.getAbsolutePath()));
        FileOutputFormat.setOutputPath(job, new Path(queryOutput.getAbsolutePath()));

        if (!job.waitForCompletion(false)) {
            throw new RuntimeException("MapReduce Query 2 failed");
        }

        return readQuery2Output(queryOutput);
    }

    public static class Query2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 8) return;

            String host = fields[0];
            String resourcePath = fields[4];
            String bytes = fields[7];

            outKey.set(resourcePath);
            outVal.set(bytes + "\t" + host);
            context.write(outKey, outVal);
        }
    }

    public static class Query2Reducer extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long requestCount = 0;
            long totalBytes = 0;
            Set<String> distinctHosts = new HashSet<>();

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                totalBytes += Long.parseLong(parts[0]);
                distinctHosts.add(parts[1]);
                requestCount++;
            }

            outVal.set(requestCount + "\t" + totalBytes + "\t" + distinctHosts.size());
            context.write(key, outVal);
        }
    }

    private List<Map<String, Object>> readQuery2Output(File outputDir) throws IOException {
        List<Map<String, Object>> allResults = new ArrayList<>();
        for (File file : outputDir.listFiles()) {
            if (file.getName().startsWith("part-")) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length >= 4) {
                            Map<String, Object> row = new LinkedHashMap<>();
                            row.put("resource_path", parts[0]);
                            row.put("request_count", Long.parseLong(parts[1]));
                            row.put("total_bytes", Long.parseLong(parts[2]));
                            row.put("distinct_host_count", Long.parseLong(parts[3]));
                            allResults.add(row);
                        }
                    }
                }
            }
        }

        // Sort by request_count descending and take top 20
        allResults.sort((a, b) -> Long.compare((Long) b.get("request_count"), (Long) a.get("request_count")));
        return allResults.subList(0, Math.min(20, allResults.size()));
    }

    // ======================== Query 3: Hourly Error Analysis ========================

    /**
     * Query 3: Hourly Error Analysis
     * For each (logDate, logHour) → error requests (400-599), total requests, error rate, distinct error hosts
     */
    @Override
    public List<Map<String, Object>> executeHourlyErrorAnalysis() throws Exception {
        File queryOutput = new File(outputDir, "query3");
        if (queryOutput.exists()) deleteDir(queryOutput);

        Job job = Job.getInstance(hadoopConf, "Query3_HourlyErrorAnalysis");
        job.setJarByClass(MapReduceDatabase.class);

        job.setMapperClass(Query3Mapper.class);
        job.setReducerClass(Query3Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputFile.getAbsolutePath()));
        FileOutputFormat.setOutputPath(job, new Path(queryOutput.getAbsolutePath()));

        if (!job.waitForCompletion(false)) {
            throw new RuntimeException("MapReduce Query 3 failed");
        }

        return readQuery3Output(queryOutput);
    }

    public static class Query3Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 8) return;

            String logDate = fields[1];
            String logHour = fields[2];
            String host = fields[0];
            int statusCode = Integer.parseInt(fields[6]);

            boolean isError = statusCode >= 400 && statusCode <= 599;

            outKey.set(logDate + "\t" + logHour);
            // Format: isError(0/1) \t host
            outVal.set((isError ? "1" : "0") + "\t" + host);
            context.write(outKey, outVal);
        }
    }

    public static class Query3Reducer extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalRequests = 0;
            long errorRequests = 0;
            Set<String> errorHosts = new HashSet<>();

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                int isError = Integer.parseInt(parts[0]);
                String host = parts[1];

                totalRequests++;
                if (isError == 1) {
                    errorRequests++;
                    errorHosts.add(host);
                }
            }

            double errorRate = totalRequests > 0 ? (double) errorRequests / totalRequests : 0.0;

            outVal.set(errorRequests + "\t" + totalRequests + "\t" +
                    String.format("%.6f", errorRate) + "\t" + errorHosts.size());
            context.write(key, outVal);
        }
    }

    private List<Map<String, Object>> readQuery3Output(File outputDir) throws IOException {
        List<Map<String, Object>> results = new ArrayList<>();
        for (File file : outputDir.listFiles()) {
            if (file.getName().startsWith("part-")) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length >= 6) {
                            Map<String, Object> row = new LinkedHashMap<>();
                            row.put("log_date", parts[0]);
                            row.put("log_hour", Integer.parseInt(parts[1]));
                            row.put("error_request_count", Long.parseLong(parts[2]));
                            row.put("total_request_count", Long.parseLong(parts[3]));
                            row.put("error_rate", Double.parseDouble(parts[4]));
                            row.put("distinct_error_hosts", Long.parseLong(parts[5]));
                            results.add(row);
                        }
                    }
                }
            }
        }
        results.sort(Comparator.comparing((Map<String, Object> m) -> (String) m.get("log_date"))
                .thenComparing(m -> (Integer) m.get("log_hour")));
        return results;
    }

    // ======================== Utilities ========================

    @Override
    public void cleanup() throws Exception {
        // Clean up temp files
        if (dataDir != null && dataDir.exists()) {
            deleteDir(dataDir);
        }
    }

    private void deleteDir(File dir) {
        if (dir.isDirectory()) {
            File[] children = dir.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteDir(child);
                }
            }
        }
        dir.delete();
    }
}
