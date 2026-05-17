package com.etl.mapreduce;

import com.etl.config.AppConfig;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MapReduce Mapper for Query 1: Daily Traffic Summary.
 * Parses raw log lines, emits (log_date\tstatus_code) -> bytes_transferred.
 * Malformed records are tracked via Hadoop counters.
 */
public class Query1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Pattern LOG_RE = Pattern.compile(AppConfig.LOG_REGEX);
    private static final String MALFORMED_COUNTER_GROUP = "ETL";
    private static final String MALFORMED_COUNTER_NAME = "malformed_records";

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        Matcher m = LOG_RE.matcher(line);
        if (!m.matches()) {
            context.getCounter(MALFORMED_COUNTER_GROUP, MALFORMED_COUNTER_NAME).increment(1);
            return;
        }

        String logDate = m.group(2);
        String statusCode = m.group(7);
        String bytesRaw = m.group(8);
        long bytesTransferred = "-".equals(bytesRaw) ? 0L : Long.parseLong(bytesRaw);

        // Key: log_date\tstatus_code   Value: bytes_transferred
        outKey.set(logDate + "\t" + statusCode);
        outVal.set(String.valueOf(bytesTransferred));
        context.write(outKey, outVal);
    }
}
