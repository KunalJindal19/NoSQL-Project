package com.etl.pipeline;

import com.etl.config.AppConfig;
import com.etl.mapreduce.Query1Mapper;
import com.etl.mapreduce.Query1Reducer;
import com.etl.mapreduce.Query2Mapper;
import com.etl.mapreduce.Query2Reducer;
import com.etl.mapreduce.Query3Mapper;
import com.etl.mapreduce.Query3Reducer;
import com.etl.model.PipelineResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * MapReduce pipeline implementation using native Java MapReduce API.
 * Runs in local mode (no HDFS, no YARN) - everything in a single JVM.
 * Parsing and cleaning happen inside the Mapper classes.
 */
public class MapReducePipeline implements Pipeline {

    @Override
    public PipelineResult execute(String batchFile, String queryName, int batchId) throws Exception {
        String outputDir = AppConfig.TEMP_OUTPUT_DIR + "/mapreduce/" + queryName + "_batch_" + batchId;
        deleteDirectory(new File(outputDir));

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        Job job = Job.getInstance(conf, "ETL-" + queryName + "-batch-" + batchId);
        job.setJarByClass(MapReducePipeline.class);

        FileInputFormat.addInputPath(job, new Path(batchFile));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        // Configure mapper/reducer based on query
        switch (queryName) {
            case "query1":
                job.setMapperClass(Query1Mapper.class);
                job.setReducerClass(Query1Reducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;
            case "query2":
                job.setMapperClass(Query2Mapper.class);
                job.setReducerClass(Query2Reducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(Text.class);
                break;
            case "query3":
                job.setMapperClass(Query3Mapper.class);
                job.setReducerClass(Query3Reducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;
            default:
                throw new IllegalArgumentException("Unknown query: " + queryName);
        }

        job.setNumReduceTasks(1); // Single reducer for local mode

        boolean success = job.waitForCompletion(true);
        if (!success) {
            throw new RuntimeException("MapReduce job failed for " + queryName);
        }

        // Read malformed count from Hadoop counters
        long malformedCount = job.getCounters()
            .findCounter("ETL", "malformed_records").getValue();

        // Read results from output directory
        List<String[]> results = readTsvOutput(outputDir);

        return new PipelineResult(results, (int) malformedCount);
    }

    /**
     * Reads tab-separated output files from a MapReduce output directory.
     * Files are named part-r-00000, etc.
     */
    private List<String[]> readTsvOutput(String outputDir) throws IOException {
        List<String[]> results = new ArrayList<String[]>();
        File dir = new File(outputDir);
        if (!dir.exists() || !dir.isDirectory()) {
            return results;
        }

        File[] files = dir.listFiles();
        if (files == null) return results;

        for (File file : files) {
            if (file.getName().startsWith("part-")) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (!line.isEmpty()) {
                            results.add(line.split("\t"));
                        }
                    }
                }
            }
        }
        return results;
    }

    /**
     * Recursively deletes a directory and all its contents.
     */
    private void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            dir.delete();
        }
    }
}
