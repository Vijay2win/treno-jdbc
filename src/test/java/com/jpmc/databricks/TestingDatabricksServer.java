package com.jpmc.databricks;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import static java.lang.String.format;

/**
 * No remote server to initialize. We might want to configure the generic service.
 */
public class TestingDatabricksServer implements Closeable {
    private static final Properties INFO = new Properties();
    {
        INFO.put("oauth-token", "sample_token");
    }

    public void execute(String sql) {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), INFO);
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    public Properties getConnectProperties() {
        return INFO;
    }

    public String getJdbcUrl() {
        return format("jdbc:hive2://%s:%s/", "localhost", "10000");
    }

    @Override
    public void close() {}
}
