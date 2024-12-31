package com.jpmc.databricks;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import java.util.Optional;

/**
 * Configuration for each JDBC connection been made. When OAuth token is recreated, this object needs to be recreated.
 */
public class DatabricksConfig {
    private String hostName;
    private String httpPath;
    private boolean mapStringAsVarchar;
    private String oauthToken;
    private String connectionUrl;

    @Config("http-path")
    public DatabricksConfig setHttpPath(String httpPath) {
        this.httpPath = httpPath;
        return this;
    }

    public String getHttpPath() {
        return httpPath;
    }

    @Config("host-name")
    @ConfigSecuritySensitive
    public DatabricksConfig setHostName(String hostName) {
        this.hostName = hostName;
        return this;
    }

    public String getHostName() {
        return hostName;
    }

    @Config("oauth-token")
    @ConfigSecuritySensitive
    public DatabricksConfig setOauthToken(String oauthToken) {
        this.oauthToken = oauthToken;
        return this;
    }

    public String getOauthToken() {
        return oauthToken;
    }

    @Config("connection-url")
    @ConfigSecuritySensitive
    public DatabricksConfig setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
        return this;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public boolean isMapStringAsVarchar()
    {
        return mapStringAsVarchar;
    }

    @Config("databricks.map-string-as-varchar")
    @ConfigDescription("Map databricks String and FixedString as varchar instead of varbinary")
    public DatabricksConfig setMapStringAsVarchar(boolean mapStringAsVarchar)
    {
        this.mapStringAsVarchar = mapStringAsVarchar;
        return this;
    }
}
