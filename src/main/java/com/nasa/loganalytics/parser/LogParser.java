package com.nasa.loganalytics.parser;

import com.nasa.loganalytics.model.LogRecord;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.GZIPInputStream;

/**
 * Parses NASA HTTP access log lines into LogRecord objects.
 * 
 * Expected format:
 *   host - - [dd/Mon/yyyy:HH:mm:ss -zone] "METHOD resource PROTOCOL" status bytes
 * 
 * Handles edge cases:
 *   - Missing bytes field (represented as "-") → treated as 0
 *   - Malformed lines → tracked and reported
 *   - Request lines without protocol version
 */
public class LogParser {

    // Regex to parse NASA log lines
    // Group 1: host
    // Group 2: timestamp (inside brackets)
    // Group 3: request string (inside quotes)
    // Group 4: status code
    // Group 5: bytes (or "-")
    private static final Pattern LOG_PATTERN = Pattern.compile(
            "^(\\S+)\\s+-\\s+-\\s+\\[(.+?)\\]\\s+\"(.+?)\"\\s+(\\d{3})\\s+(\\S+)$"
    );

    // Month name to number mapping
    private static final Map<String, String> MONTH_MAP = new HashMap<>();
    static {
        MONTH_MAP.put("Jan", "01"); MONTH_MAP.put("Feb", "02");
        MONTH_MAP.put("Mar", "03"); MONTH_MAP.put("Apr", "04");
        MONTH_MAP.put("May", "05"); MONTH_MAP.put("Jun", "06");
        MONTH_MAP.put("Jul", "07"); MONTH_MAP.put("Aug", "08");
        MONTH_MAP.put("Sep", "09"); MONTH_MAP.put("Oct", "10");
        MONTH_MAP.put("Nov", "11"); MONTH_MAP.put("Dec", "12");
    }

    private int malformedCount = 0;

    /**
     * Parse a single log line into a LogRecord.
     */
    public LogRecord parseLine(String line) {
        LogRecord record = new LogRecord();

        if (line == null || line.trim().isEmpty()) {
            record.setMalformed(true);
            malformedCount++;
            return record;
        }

        Matcher matcher = LOG_PATTERN.matcher(line.trim());
        if (!matcher.matches()) {
            record.setMalformed(true);
            malformedCount++;
            return record;
        }

        try {
            // Host
            record.setHost(matcher.group(1));

            // Timestamp parsing
            String rawTimestamp = matcher.group(2);
            record.setTimestamp(rawTimestamp);
            parseTimestamp(record, rawTimestamp);

            // Request string parsing: "METHOD resource PROTOCOL"
            String requestStr = matcher.group(3);
            parseRequest(record, requestStr);

            // Status code
            record.setStatusCode(Integer.parseInt(matcher.group(4)));

            // Bytes transferred (handle "-")
            String bytesStr = matcher.group(5);
            if ("-".equals(bytesStr)) {
                record.setBytesTransferred(0);
            } else {
                record.setBytesTransferred(Long.parseLong(bytesStr));
            }

            record.setMalformed(false);
        } catch (Exception e) {
            record.setMalformed(true);
            malformedCount++;
        }

        return record;
    }

    /**
     * Parse timestamp string like "01/Jul/1995:00:00:01 -0400"
     * into logDate (yyyy-MM-dd) and logHour (int 0-23).
     */
    private void parseTimestamp(LogRecord record, String timestamp) {
        // Format: dd/Mon/yyyy:HH:mm:ss -zone
        // Example: 01/Jul/1995:00:00:01 -0400
        String day = timestamp.substring(0, 2);
        String monthStr = timestamp.substring(3, 6);
        String year = timestamp.substring(7, 11);
        String hourStr = timestamp.substring(12, 14);

        String month = MONTH_MAP.getOrDefault(monthStr, "01");
        record.setLogDate(year + "-" + month + "-" + day);
        record.setLogHour(Integer.parseInt(hourStr));
    }

    /**
     * Parse the request string: "METHOD resource PROTOCOL"
     * Handles cases where protocol may be missing.
     */
    private void parseRequest(LogRecord record, String requestStr) {
        String[] parts = requestStr.split("\\s+");
        if (parts.length >= 1) {
            record.setHttpMethod(parts[0]);
        }
        if (parts.length >= 2) {
            record.setResourcePath(parts[1]);
        } else {
            record.setResourcePath("/");
        }
        if (parts.length >= 3) {
            record.setProtocolVersion(parts[2]);
        } else {
            record.setProtocolVersion("");
        }
    }

    /**
     * Read all log records from gzipped files, processing in batches.
     * Returns batches of parsed LogRecords.
     */
    public List<List<LogRecord>> readAndBatch(List<File> files, int batchSize) throws IOException {
        List<List<LogRecord>> batches = new ArrayList<>();
        List<LogRecord> currentBatch = new ArrayList<>();

        for (File file : files) {
            try (BufferedReader reader = createReader(file)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    LogRecord record = parseLine(line);
                    currentBatch.add(record);

                    if (currentBatch.size() >= batchSize) {
                        batches.add(currentBatch);
                        currentBatch = new ArrayList<>();
                    }
                }
            }
        }

        // Add remaining records as the final batch
        if (!currentBatch.isEmpty()) {
            batches.add(currentBatch);
        }

        return batches;
    }

    /**
     * Creates an appropriate reader for .gz or plain text files.
     */
    private BufferedReader createReader(File file) throws IOException {
        if (file.getName().endsWith(".gz")) {
            return new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(new FileInputStream(file))));
        } else {
            return new BufferedReader(new FileReader(file));
        }
    }

    public int getMalformedCount() {
        return malformedCount;
    }

    public void resetMalformedCount() {
        this.malformedCount = 0;
    }
}
