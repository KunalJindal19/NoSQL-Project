package com.etl.db;

import com.etl.config.AppConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Manages MySQL JDBC connections.
 */
public class ConnectionManager {

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("MySQL JDBC driver not found", e);
        }
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
            AppConfig.MYSQL_URL,
            AppConfig.MYSQL_USER,
            AppConfig.MYSQL_PASSWORD
        );
    }
}
