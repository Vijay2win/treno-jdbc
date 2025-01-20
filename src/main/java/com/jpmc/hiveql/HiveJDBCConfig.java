package com.jpmc.hiveql;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

/**
 * Configuration for each JDBC connection been made. When OAuth token is recreated, this object needs to be recreated.
 */
public class HiveJDBCConfig {
    private String httpPath;
    private boolean mapStringAsVarchar;
    private String connectionUrl;

    @Config("http-path")
    public HiveJDBCConfig setHttpPath(String httpPath) {
        this.httpPath = httpPath;
        return this;
    }

    public String getHttpPath() {
        return httpPath;
    }

    @Config("connection-url")
    @ConfigSecuritySensitive
    public HiveJDBCConfig setConnectionUrl(String connectionUrl) {
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
    public HiveJDBCConfig setMapStringAsVarchar(boolean mapStringAsVarchar)
    {
        this.mapStringAsVarchar = mapStringAsVarchar;
        return this;
    }
}
