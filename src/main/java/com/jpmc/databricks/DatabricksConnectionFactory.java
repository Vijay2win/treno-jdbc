package com.jpmc.databricks;

import io.airlift.log.Logger;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Main class for JDBC connector.
 */
public class DatabricksConnectionFactory implements ConnectionFactory {
    private static final Logger log = Logger.get(DatabricksConnectionFactory.class);
    private final DatabricksConfig config;

    public DatabricksConnectionFactory(DatabricksConfig config) {
        this.config = config;
    }

    @Override
    public Connection openConnection(ConnectorSession session) throws SQLException {
        try {
            // First check the extra creds
            String token = session.getIdentity().getExtraCredentials().get("oauth-token");
            if (token == null) {
                // check the session property if not found on extra cred.
                token = session.getProperty("oauth-token", String.class);
            }

            // Handle if the connection url is sent as a whole
            if (config.getConnectionUrl() != null && token == null) {
                String url = config.getConnectionUrl();
                return DriverManager.getConnection(url);
            }
            /*
             *  jdbc:databricks://<server-hostname>:443;
             *      httpPath=<http-path>;
             *      AuthMech=11;
             *      Auth_Flow=0;
             *      Auth_AccessToken=<oauth-token>
             */
            Class.forName("com.databricks.client.jdbc.Driver");
            String url = "jdbc:databricks://" + config.getHostName() + ":443";
            Properties properties = new Properties();
            properties.put("httpPath", config.getHttpPath());
            properties.put("AuthMech", "11");
            properties.put("Auth_Flow", "0");
            properties.put("Auth_AccessToken", token);
            return DriverManager.getConnection(url, properties);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }
}
