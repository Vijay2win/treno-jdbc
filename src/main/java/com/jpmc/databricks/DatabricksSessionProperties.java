package com.jpmc.databricks;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

/**
 * TODO figure out if this is required, we already have connection config thats good enough for our customization.
 */
public class DatabricksSessionProperties implements SessionPropertiesProvider {
    public static final String MAP_STRING_AS_VARCHAR = "map_string_as_varchar";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public DatabricksSessionProperties(DatabricksConfig clickHouseConfig) {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        MAP_STRING_AS_VARCHAR,
                        "Map JDBC String and FixedString as varchar instead of varbinary",
                        clickHouseConfig.isMapStringAsVarchar(),
                        true));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties() {
        return sessionProperties;
    }

    public static boolean isMapStringAsVarchar(ConnectorSession session) {
        return session.getProperty(MAP_STRING_AS_VARCHAR, Boolean.class);
    }
}
