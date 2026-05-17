package com.etl.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * MapReduce Reducer for Query 1: Daily Traffic Summary.
 * Aggregates: COUNT requests and SUM bytes per (log_date, status_code).
 * Input key: "log_date\tstatus_code", values: bytes_transferred
 * Output: "log_date\tstatus_code\trequest_count\ttotal_bytes"
 */
public class Query1Reducer extends Reducer<Text, Text, Text, Text> {

    private final Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        long requestCount = 0;
        long totalBytes = 0;

        for (Text val : values) {
            requestCount++;
            totalBytes += Long.parseLong(val.toString());
        }

        // Output: log_date \t status_code \t request_count \t total_bytes
        outVal.set(requestCount + "\t" + totalBytes);
        context.write(key, outVal);
    }
}
