package com.nasa.loganalytics.model;

/**
 * POJO representing a single parsed NASA HTTP access log entry.
 * 
 * Log format: host - - [timestamp] "method resource protocol" statusCode bytes
 * Example: 199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
 */
public class LogRecord {

    private String host;
    private String timestamp;       // raw timestamp string, e.g. "01/Jul/1995:00:00:01 -0400"
    private String logDate;         // yyyy-MM-dd, e.g. "1995-07-01"
    private int logHour;            // 0–23
    private String httpMethod;      // GET, POST, HEAD, etc.
    private String resourcePath;    // e.g. /history/apollo/
    private String protocolVersion; // e.g. HTTP/1.0
    private int statusCode;         // e.g. 200, 404
    private long bytesTransferred;  // 0 if missing or "-"
    private boolean malformed;      // true if the original line could not be parsed

    public LogRecord() {}

    // --- Getters and Setters ---

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getLogDate() {
        return logDate;
    }

    public void setLogDate(String logDate) {
        this.logDate = logDate;
    }

    public int getLogHour() {
        return logHour;
    }

    public void setLogHour(int logHour) {
        this.logHour = logHour;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public void setHttpMethod(String httpMethod) {
        this.httpMethod = httpMethod;
    }

    public String getResourcePath() {
        return resourcePath;
    }

    public void setResourcePath(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(String protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public long getBytesTransferred() {
        return bytesTransferred;
    }

    public void setBytesTransferred(long bytesTransferred) {
        this.bytesTransferred = bytesTransferred;
    }

    public boolean isMalformed() {
        return malformed;
    }

    public void setMalformed(boolean malformed) {
        this.malformed = malformed;
    }

    @Override
    public String toString() {
        if (malformed) {
            return "LogRecord{MALFORMED}";
        }
        return String.format("LogRecord{host='%s', date='%s', hour=%d, method='%s', resource='%s', status=%d, bytes=%d}",
                host, logDate, logHour, httpMethod, resourcePath, statusCode, bytesTransferred);
    }
}
