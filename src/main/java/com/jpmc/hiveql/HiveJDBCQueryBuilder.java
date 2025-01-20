package com.jpmc.hiveql;

import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;

public class HiveJDBCQueryBuilder extends DefaultQueryBuilder {
    public HiveJDBCQueryBuilder(RemoteQueryModifier queryModifier) {
        super(queryModifier);
    }
}
