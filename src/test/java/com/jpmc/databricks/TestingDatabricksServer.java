package com.jpmc.databricks;

import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import static java.lang.String.format;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

/**
 * No remote server to initialize. We might want to configure the generic service.
 */
public class TestingDatabricksServer implements Closeable {
    private static final DockerImageName HIVE_IMAGE = DockerImageName.parse("apache/hive");
    public static final DockerImageName HIVE_DEFAULT_IMAGE = HIVE_IMAGE.withTag("4.0.13");

    private ClickHouseContainer dockerContainer;

    public TestingDatabricksServer() {
        this(HIVE_DEFAULT_IMAGE);
    }

    public TestingDatabricksServer(DockerImageName image) {
//        dockerContainer = new ClickHouseContainer(image)
//                .withExposedPorts(10000, 10001, 10002)
//                .withEnv("SERVICE_NAME", "hiveserver2")
//                .withEnv("HIVE_SERVER2_TRANSPORT_MODE", "all")
//                .withStartupAttempts(10);
//        dockerContainer.start();

        // Comment the above to connect to local hive if needed.
    }

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
        return format("jdbc:hive2://%s:%s/default;auth=noSasl;SSL=0;transportMode=http;httpPath=cliservice;user=anonymous;password=anonymous", "localhost", "10001");
    }

    @Override
    public void close() {
    }
}
