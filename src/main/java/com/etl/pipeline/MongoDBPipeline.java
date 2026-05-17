package com.etl.pipeline;

import com.etl.config.AppConfig;
import com.etl.model.PipelineResult;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * MongoDB pipeline implementation using the MongoDB Java Sync Driver (4.11.x).
 * Raw log lines are inserted as-is into MongoDB. ALL parsing, cleaning, and
 * aggregation happens inside the MongoDB Aggregation Framework using $regexFind.
 * NO Java-side log parsing is performed.
 */
public class MongoDBPipeline implements Pipeline {

    private static final String REGEX_PATTERN =
        "^(\\S+)\\s+-\\s+-\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+-\\d{4}\\]" +
        "\\s+\"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?\"\\s+(\\d{3})\\s+(\\S+)$";

    @Override
    public PipelineResult execute(String batchFile, String queryName, int batchId) throws Exception {
        try (MongoClient client = MongoClients.create(AppConfig.MONGO_CONNECTION_STRING)) {
            MongoDatabase db = client.getDatabase(AppConfig.MONGO_DATABASE);
            String collectionName = "batch_" + batchId;
            MongoCollection<Document> collection = db.getCollection(collectionName);
            collection.drop(); // Clean slate

            // Phase 1: Insert Raw Lines ONLY (No Java Parsing!)
            List<Document> batch = new ArrayList<Document>();

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(new FileInputStream(batchFile), AppConfig.LOG_FILE_ENCODING))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    // Insert the raw string as a single field. MongoDB does the parsing.
                    batch.add(new Document("raw_log_line", line));

                    if (batch.size() >= 10000) { // Bulk insert for performance
                        collection.insertMany(new ArrayList<Document>(batch));
                        batch.clear();
                    }
                }
            }
            if (!batch.isEmpty()) {
                collection.insertMany(batch);
            }

            // Count malformed records using MongoDB (lines that don't match the regex)
            Document malformedQuery = new Document("$expr", new Document("$eq", Arrays.asList(
                new Document("$regexFind", new Document()
                    .append("input", "$raw_log_line")
                    .append("regex", REGEX_PATTERN)
                ),
                null
            )));
            int malformedCount = (int) collection.countDocuments(malformedQuery);

            // Phase 2: Run native aggregation query
            List<String[]> results;
            switch (queryName) {
                case "query1": results = runQuery1(collection); break;
                case "query2": results = runQuery2(collection); break;
                case "query3": results = runQuery3(collection); break;
                default: throw new IllegalArgumentException("Unknown query: " + queryName);
            }

            // Clean up collection after reading results
            collection.drop();
            return new PipelineResult(results, malformedCount);
        }
    }

    /**
     * Returns the base parsing stages for the MongoDB aggregation pipeline.
     * These stages: 1) parse with regex, 2) filter malformed, 3) extract fields, 4) clean bytes.
     */
    private List<Document> getBaseParsingStages() {
        List<Document> stages = new ArrayList<Document>();

        // 1. Parsing Stage: Apply regex inside MongoDB
        stages.add(new Document("$addFields", new Document("parsed",
            new Document("$regexFind", new Document()
                .append("input", "$raw_log_line")
                .append("regex", REGEX_PATTERN)
            )
        )));

        // 2. Filter Stage: Drop malformed records
        stages.add(new Document("$match", new Document("parsed", new Document("$ne", null))));

        // 3. Extraction Stage: Pull capture groups into fields
        stages.add(new Document("$addFields", new Document()
            .append("host", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 0)))
            .append("log_date", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 1)))
            .append("log_hour", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 2)))
            .append("http_method", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 3)))
            .append("resource_path", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 4)))
            .append("protocol_version", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 5)))
            .append("status_code", new Document("$toInt", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 6))))
            .append("bytes_raw", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 7)))
        ));

        // 4. Cleaning Stage: Convert "-" bytes to 0
        stages.add(new Document("$addFields", new Document("bytes_transferred",
            new Document("$cond", Arrays.asList(
                new Document("$eq", Arrays.asList("$bytes_raw", "-")),
                0L,
                new Document("$toLong", "$bytes_raw")
            ))
        )));

        return stages;
    }

    /**
     * Query 1: Daily Traffic Summary
     * GROUP BY (log_date, status_code) -> COUNT, SUM(bytes)
     */
    private List<String[]> runQuery1(MongoCollection<Document> collection) {
        List<Document> pipeline = new ArrayList<Document>(getBaseParsingStages());

        pipeline.addAll(Arrays.asList(
            new Document("$group", new Document()
                .append("_id", new Document()
                    .append("log_date", "$log_date")
                    .append("status_code", "$status_code"))
                .append("request_count", new Document("$sum", 1))
                .append("total_bytes", new Document("$sum", "$bytes_transferred"))),
            new Document("$project", new Document()
                .append("_id", 0)
                .append("log_date", "$_id.log_date")
                .append("status_code", "$_id.status_code")
                .append("request_count", 1)
                .append("total_bytes", 1)),
            new Document("$sort", new Document()
                .append("log_date", 1)
                .append("status_code", 1))
        ));

        List<String[]> results = new ArrayList<String[]>();
        for (Document doc : collection.aggregate(pipeline).allowDiskUse(true)) {
            results.add(new String[]{
                doc.getString("log_date"),
                String.valueOf(doc.getInteger("status_code")),
                String.valueOf(doc.getInteger("request_count")),
                String.valueOf(getLongValue(doc, "total_bytes"))
            });
        }
        return results;
    }

    /**
     * Query 2: Top Requested Resources
     * GROUP BY resource_path -> COUNT, SUM(bytes), COUNT(DISTINCT host) -> ORDER DESC -> LIMIT 20
     */
    private List<String[]> runQuery2(MongoCollection<Document> collection) {
        List<Document> pipeline = new ArrayList<Document>(getBaseParsingStages());

        pipeline.addAll(Arrays.asList(
            new Document("$group", new Document()
                .append("_id", "$resource_path")
                .append("request_count", new Document("$sum", 1))
                .append("total_bytes", new Document("$sum", "$bytes_transferred"))
                .append("distinct_hosts", new Document("$addToSet", "$host"))),
            new Document("$project", new Document()
                .append("_id", 0)
                .append("resource_path", "$_id")
                .append("request_count", 1)
                .append("total_bytes", 1)
                .append("distinct_host_count", new Document("$size", "$distinct_hosts"))),
            new Document("$sort", new Document("request_count", -1)),
            new Document("$limit", 20)
        ));

        List<String[]> results = new ArrayList<String[]>();
        for (Document doc : collection.aggregate(pipeline).allowDiskUse(true)) {
            results.add(new String[]{
                doc.getString("resource_path"),
                String.valueOf(doc.getInteger("request_count")),
                String.valueOf(getLongValue(doc, "total_bytes")),
                String.valueOf(doc.getInteger("distinct_host_count"))
            });
        }
        return results;
    }

    /**
     * Query 3: Hourly Error Analysis
     * GROUP BY (log_date, log_hour) -> error count, total count, error rate, distinct error hosts
     */
    private List<String[]> runQuery3(MongoCollection<Document> collection) {
        List<Document> pipeline = new ArrayList<Document>(getBaseParsingStages());

        pipeline.addAll(Arrays.asList(
            new Document("$group", new Document()
                .append("_id", new Document()
                    .append("log_date", "$log_date")
                    .append("log_hour", "$log_hour"))
                .append("total_request_count", new Document("$sum", 1))
                .append("error_request_count", new Document("$sum",
                    new Document("$cond", Arrays.asList(
                        new Document("$and", Arrays.asList(
                            new Document("$gte", Arrays.asList("$status_code", 400)),
                            new Document("$lte", Arrays.asList("$status_code", 599))
                        )),
                        1, 0
                    ))))
                .append("error_hosts", new Document("$addToSet",
                    new Document("$cond", Arrays.asList(
                        new Document("$and", Arrays.asList(
                            new Document("$gte", Arrays.asList("$status_code", 400)),
                            new Document("$lte", Arrays.asList("$status_code", 599))
                        )),
                        "$host", "$$REMOVE"
                    ))))),
            new Document("$project", new Document()
                .append("_id", 0)
                .append("log_date", "$_id.log_date")
                .append("log_hour", "$_id.log_hour")
                .append("error_request_count", 1)
                .append("total_request_count", 1)
                .append("error_rate", new Document("$cond", Arrays.asList(
                    new Document("$eq", Arrays.asList("$total_request_count", 0)),
                    0,
                    new Document("$divide", Arrays.asList("$error_request_count", "$total_request_count"))
                )))
                .append("distinct_error_hosts", new Document("$size", "$error_hosts"))),
            new Document("$sort", new Document()
                .append("log_date", 1)
                .append("log_hour", 1))
        ));

        List<String[]> results = new ArrayList<String[]>();
        for (Document doc : collection.aggregate(pipeline).allowDiskUse(true)) {
            results.add(new String[]{
                doc.getString("log_date"),
                doc.getString("log_hour"),
                String.valueOf(doc.getInteger("error_request_count")),
                String.valueOf(doc.getInteger("total_request_count")),
                String.format("%.6f", getDoubleValue(doc, "error_rate")),
                String.valueOf(doc.getInteger("distinct_error_hosts"))
            });
        }
        return results;
    }

    /**
     * Safely extracts a long value from a Document, handling both Integer and Long types.
     */
    private long getLongValue(Document doc, String field) {
        Object val = doc.get(field);
        if (val instanceof Long) {
            return (Long) val;
        } else if (val instanceof Integer) {
            return ((Integer) val).longValue();
        } else if (val instanceof Double) {
            return ((Double) val).longValue();
        }
        return 0L;
    }

    /**
     * Safely extracts a double value from a Document.
     */
    private double getDoubleValue(Document doc, String field) {
        Object val = doc.get(field);
        if (val instanceof Double) {
            return (Double) val;
        } else if (val instanceof Integer) {
            return ((Integer) val).doubleValue();
        } else if (val instanceof Long) {
            return ((Long) val).doubleValue();
        }
        return 0.0;
    }
}
