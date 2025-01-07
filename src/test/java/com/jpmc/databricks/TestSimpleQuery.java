package com.jpmc.databricks;

import org.junit.jupiter.api.Test;

import java.sql.*;

public class TestSimpleQuery {
    @Test
    public void testAlreadyRunningService() {
        TestingDatabricksServer runner = new TestingDatabricksServer();
        try (Connection connection = DriverManager.getConnection(runner.getJdbcUrl(), runner.getConnectProperties());
             Statement statement = connection.createStatement()) {
            statement.execute("select 1");
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: ", e);
        }
    }
}
