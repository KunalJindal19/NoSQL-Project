package com.etl.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * MapReduce Reducer for Query 2: Top Requested Resources.
 * Collects all resource stats, sorts by count DESC, outputs top 20 in cleanup().
 * Input key: resource_path, values: "host|bytes"
 * Output: top 20 resources by request count
 */
public class Query2Reducer extends Reducer<Text, Text, NullWritable, Text> {

    // Accumulate all resource stats across reduce() calls, output top 20 in cleanup()
    private Map<String, long[]> resourceStats = new HashMap<String, long[]>();
    private Map<String, Set<String>> resourceHosts = new HashMap<String, Set<String>>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String resource = key.toString();
        long count = 0;
        long totalBytes = 0;
        Set<String> hosts = new HashSet<String>();

        for (Text val : values) {
            String[] parts = val.toString().split("\\|", 2);
            hosts.add(parts[0]);
            totalBytes += Long.parseLong(parts[1]);
            count++;
        }

        resourceStats.put(resource, new long[]{count, totalBytes});
        resourceHosts.put(resource, hosts);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Sort by request count descending, take top 20
        List<Map.Entry<String, long[]>> sorted = new ArrayList<Map.Entry<String, long[]>>(resourceStats.entrySet());
        java.util.Collections.sort(sorted, new java.util.Comparator<Map.Entry<String, long[]>>() {
            @Override
            public int compare(Map.Entry<String, long[]> a, Map.Entry<String, long[]> b) {
                return Long.compare(b.getValue()[0], a.getValue()[0]);
            }
        });

        int limit = Math.min(20, sorted.size());
        for (int i = 0; i < limit; i++) {
            Map.Entry<String, long[]> entry = sorted.get(i);
            String resource = entry.getKey();
            long[] stats = entry.getValue();
            int distinctHosts = resourceHosts.get(resource).size();
            context.write(NullWritable.get(),
                new Text(resource + "\t" + stats[0] + "\t" + stats[1] + "\t" + distinctHosts));
        }
    }
}
