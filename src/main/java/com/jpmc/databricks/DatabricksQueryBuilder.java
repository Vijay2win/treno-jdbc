package com.jpmc.databricks;

import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;

public class DatabricksQueryBuilder extends DefaultQueryBuilder {
    public DatabricksQueryBuilder(RemoteQueryModifier queryModifier) {
        super(queryModifier);
    }
}
