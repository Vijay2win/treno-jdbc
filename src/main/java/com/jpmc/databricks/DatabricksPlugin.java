package com.jpmc.databricks;

import io.trino.plugin.jdbc.JdbcPlugin;

/**
 * Plugin configuration
 */
public class DatabricksPlugin extends JdbcPlugin {
    public DatabricksPlugin() {
        super("databricks", DatabricksClientModule::new);
    }
}
