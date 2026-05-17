package com.etl.mapreduce;

import com.etl.config.AppConfig;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MapReduce Mapper for Query 2: Top Requested Resources.
 * Parses raw log lines, emits resource_path -> "host|bytes".
 */
public class Query2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Pattern LOG_RE = Pattern.compile(AppConfig.LOG_REGEX);

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        Matcher m = LOG_RE.matcher(line);
        if (!m.matches()) {
            context.getCounter("ETL", "malformed_records").increment(1);
            return;
        }

        String host = m.group(1);
        String resourcePath = m.group(5);
        String bytesRaw = m.group(8);
        long bytes = "-".equals(bytesRaw) ? 0L : Long.parseLong(bytesRaw);

        // Key: resource_path   Value: host|bytes
        outKey.set(resourcePath);
        outVal.set(host + "|" + bytes);
        context.write(outKey, outVal);
    }
}
