package com.jpmc.hiveql;

import io.trino.plugin.jdbc.JdbcPlugin;

/**
 * Plugin configuration
 */
public class HiveJDBCPlugin extends JdbcPlugin {
    public HiveJDBCPlugin() {
        super("databricks", HiveJDBCModule::new);
    }
}
