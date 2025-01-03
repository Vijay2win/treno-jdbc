package com.jpmc.databricks;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class DatabricksDriver implements Driver {
    private final com.databricks.client.jdbc.Driver databricksDelegate;
    private final org.apache.hive.jdbc.HiveDriver hiveDelegate;
    private final DatabricksConfig config;

    public DatabricksDriver(DatabricksConfig config) {
        this.config = config;
        this.databricksDelegate = new com.databricks.client.jdbc.Driver();
        this.hiveDelegate = new org.apache.hive.jdbc.HiveDriver();
    }
    @Override
    public Connection connect(String url, Properties properties) throws SQLException {
        try {

            // First check the extra creds
            String token = properties.getProperty(OauthCredentialPropertiesProvider.OAUTH_TOKEN_NAME, null);
            if (token == null) {
                // check the session property if not found on extra cred.
                // token = session.getProperty("oauth-token", String.class);
            }

            // Handle if the connection url is sent as a whole
            if (config.getConnectionUrl() != null) { // TODO add oauth check
                return hiveDelegate.connect(url, properties);
            }
            /*
             *  jdbc:databricks://<server-hostname>:443;
             *      httpPath=<http-path>;
             *      AuthMech=11;
             *      Auth_Flow=0;
             *      Auth_AccessToken=<oauth-token>
             */
            Class.forName("com.databricks.client.jdbc.Driver");
            String newurl = "jdbc:databricks://" + config.getHostName() + ":443";
            Properties newproperties = new Properties();
            newproperties.put("httpPath", config.getHttpPath());
            newproperties.put("AuthMech", "11");
            newproperties.put("Auth_Flow", "0");
            newproperties.put("Auth_AccessToken", token);
            return databricksDelegate.connect(newurl, newproperties);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && (url.startsWith("jdbc:hive2:") || databricksDelegate.acceptsURL(url));
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) throws SQLException {
        return databricksDelegate.getPropertyInfo(url, properties);
    }

    @Override
    public int getMajorVersion() {
        return databricksDelegate.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return databricksDelegate.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return databricksDelegate.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return databricksDelegate.getParentLogger();
    }
}
