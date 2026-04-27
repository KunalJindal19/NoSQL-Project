package com.nasa.loganalytics.db;

import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.nasa.loganalytics.model.LogRecord;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.*;

/**
 * MongoDB pipeline implementation using the MongoDB Aggregation Framework.
 * 
 * Data is loaded into a 'logs' collection in the 'nasa_logs' database.
 * Queries are implemented as aggregation pipelines.
 */
public class MongoDBDatabase extends Database {

    private final String mongoHost;
    private final int mongoPort;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    public MongoDBDatabase() {
        this("localhost", 27017);
    }

    public MongoDBDatabase(String host, int port) {
        super("MongoDB");
        this.mongoHost = host;
        this.mongoPort = port;
    }

    @Override
    public void initialize() throws Exception {
        String connStr = String.format("mongodb://%s:%d", mongoHost, mongoPort);
        mongoClient = MongoClients.create(connStr);
        database = mongoClient.getDatabase("nasa_logs");

        // Drop and recreate the collection for a clean run
        database.getCollection("logs").drop();
        collection = database.getCollection("logs");

        // Create indexes for query performance
        collection.createIndex(Indexes.compoundIndex(
                Indexes.ascending("logDate"),
                Indexes.ascending("statusCode")));
        collection.createIndex(Indexes.ascending("resourcePath"));
        collection.createIndex(Indexes.compoundIndex(
                Indexes.ascending("logDate"),
                Indexes.ascending("logHour")));
    }

    @Override
    public void loadData(List<LogRecord> batch, int batchId) throws Exception {
        List<Document> documents = new ArrayList<>(batch.size());

        for (LogRecord record : batch) {
            if (record.isMalformed()) continue; // skip malformed records

            Document doc = new Document()
                    .append("host", record.getHost())
                    .append("timestamp", record.getTimestamp())
                    .append("logDate", record.getLogDate())
                    .append("logHour", record.getLogHour())
                    .append("httpMethod", record.getHttpMethod())
                    .append("resourcePath", record.getResourcePath())
                    .append("protocolVersion", record.getProtocolVersion())
                    .append("statusCode", record.getStatusCode())
                    .append("bytesTransferred", record.getBytesTransferred());

            documents.add(doc);
        }

        if (!documents.isEmpty()) {
            collection.insertMany(documents, new InsertManyOptions().ordered(false));
        }
    }

    /**
     * Query 1: Daily Traffic Summary
     * GROUP BY (logDate, statusCode) → SUM(requests), SUM(bytes)
     */
    @Override
    public List<Map<String, Object>> executeDailyTrafficSummary() throws Exception {
        List<Map<String, Object>> results = new ArrayList<>();

        List<Bson> pipeline = Arrays.asList(
                group(
                        new Document("logDate", "$logDate").append("statusCode", "$statusCode"),
                        sum("request_count", 1),
                        sum("total_bytes", "$bytesTransferred")
                ),
                sort(orderBy(ascending("_id.logDate"), ascending("_id.statusCode")))
        );

        for (Document doc : collection.aggregate(pipeline)) {
            Document id = doc.get("_id", Document.class);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("log_date", id.getString("logDate"));
            row.put("status_code", id.getInteger("statusCode"));
            row.put("request_count", doc.getInteger("request_count"));
            row.put("total_bytes", doc.get("total_bytes"));
            results.add(row);
        }

        return results;
    }

    /**
     * Query 2: Top 20 Requested Resources
     * GROUP BY resourcePath → SUM(requests), SUM(bytes), COUNT_DISTINCT(hosts)
     * Sort by request_count DESC, limit 20
     */
    @Override
    public List<Map<String, Object>> executeTopRequestedResources() throws Exception {
        List<Map<String, Object>> results = new ArrayList<>();

        List<Bson> pipeline = Arrays.asList(
                group("$resourcePath",
                        sum("request_count", 1),
                        sum("total_bytes", "$bytesTransferred"),
                        addToSet("distinct_hosts", "$host")
                ),
                // Project to compute distinct host count from the set
                new Document("$project", new Document()
                        .append("resource_path", "$_id")
                        .append("request_count", 1)
                        .append("total_bytes", 1)
                        .append("distinct_host_count", new Document("$size", "$distinct_hosts"))
                ),
                sort(descending("request_count")),
                limit(20)
        );

        for (Document doc : collection.aggregate(pipeline).allowDiskUse(true)) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("resource_path", doc.getString("resource_path"));
            row.put("request_count", doc.getInteger("request_count"));
            row.put("total_bytes", doc.get("total_bytes"));
            row.put("distinct_host_count", doc.getInteger("distinct_host_count"));
            results.add(row);
        }

        return results;
    }

    /**
     * Query 3: Hourly Error Analysis
     * GROUP BY (logDate, logHour) →
     *   COUNT(status 400-599) as error_count,
     *   COUNT(*) as total_count,
     *   error_rate = error_count / total_count,
     *   COUNT_DISTINCT(hosts with error) as distinct_error_hosts
     */
    @Override
    public List<Map<String, Object>> executeHourlyErrorAnalysis() throws Exception {
        List<Map<String, Object>> results = new ArrayList<>();

        List<Bson> pipeline = Arrays.asList(
                // Group by date + hour
                group(
                        new Document("logDate", "$logDate").append("logHour", "$logHour"),
                        sum("total_request_count", 1),
                        sum("error_request_count",
                                new Document("$cond", Arrays.asList(
                                        new Document("$and", Arrays.asList(
                                                new Document("$gte", Arrays.asList("$statusCode", 400)),
                                                new Document("$lte", Arrays.asList("$statusCode", 599))
                                        )),
                                        1,
                                        0
                                ))
                        ),
                        addToSet("error_hosts",
                                new Document("$cond", Arrays.asList(
                                        new Document("$and", Arrays.asList(
                                                new Document("$gte", Arrays.asList("$statusCode", 400)),
                                                new Document("$lte", Arrays.asList("$statusCode", 599))
                                        )),
                                        "$host",
                                        null
                                ))
                        )
                ),
                // Compute error rate and distinct error host count
                new Document("$project", new Document()
                        .append("log_date", "$_id.logDate")
                        .append("log_hour", "$_id.logHour")
                        .append("error_request_count", 1)
                        .append("total_request_count", 1)
                        .append("error_rate", new Document("$cond", Arrays.asList(
                                new Document("$eq", Arrays.asList("$total_request_count", 0)),
                                0.0,
                                new Document("$divide", Arrays.asList("$error_request_count", "$total_request_count"))
                        )))
                        .append("distinct_error_hosts", new Document("$size",
                                new Document("$filter", new Document()
                                        .append("input", "$error_hosts")
                                        .append("as", "h")
                                        .append("cond", new Document("$ne", Arrays.asList("$$h", null)))
                                )
                        ))
                ),
                sort(orderBy(ascending("log_date"), ascending("log_hour")))
        );

        for (Document doc : collection.aggregate(pipeline).allowDiskUse(true)) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("log_date", doc.getString("log_date"));
            row.put("log_hour", doc.get("log_hour"));
            row.put("error_request_count", doc.get("error_request_count"));
            row.put("total_request_count", doc.get("total_request_count"));
            row.put("error_rate", doc.get("error_rate"));
            row.put("distinct_error_hosts", doc.get("distinct_error_hosts"));
            results.add(row);
        }

        return results;
    }

    @Override
    public void cleanup() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
