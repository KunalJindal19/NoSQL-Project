package com.etl.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * MapReduce Reducer for Query 3: Hourly Error Analysis.
 * Counts errors (status 400-599), total requests, distinct error hosts, error rate.
 * Input key: "log_date\tlog_hour", values: "status_code|host"
 * Output: "log_date\tlog_hour\terror_count\ttotal_count\terror_rate\tdistinct_error_hosts"
 */
public class Query3Reducer extends Reducer<Text, Text, Text, Text> {

    private final Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        long totalCount = 0;
        long errorCount = 0;
        Set<String> errorHosts = new HashSet<String>();

        for (Text val : values) {
            String[] parts = val.toString().split("\\|", 2);
            int statusCode = Integer.parseInt(parts[0]);
            String host = parts[1];
            totalCount++;
            if (statusCode >= 400 && statusCode <= 599) {
                errorCount++;
                errorHosts.add(host);
            }
        }

        double errorRate = totalCount > 0 ? (double) errorCount / totalCount : 0.0;
        // Output: log_date \t log_hour \t error_count \t total_count \t error_rate \t distinct_error_hosts
        outVal.set(errorCount + "\t" + totalCount + "\t" +
            String.format("%.6f", errorRate) + "\t" + errorHosts.size());
        context.write(key, outVal);
    }
}
