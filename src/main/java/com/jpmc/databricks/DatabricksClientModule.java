package com.jpmc.databricks;

import com.google.inject.Module;
import com.google.inject.*;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;

/**
 * Main module configuration
 */
public class DatabricksClientModule implements Module {

    @Override
    public void configure(Binder binder) {
        configBinder(binder).bindConfig(DatabricksConfig.class);
        bindSessionPropertiesProvider(binder, DatabricksSessionProperties.class);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(DatabricksClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        binder.install(new DecimalModule());
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Singleton
    @Provides
    @ForBaseJdbc
    public ConnectionFactory getConnectionFactory(DatabricksConfig config) {
        return new DatabricksConnectionFactory(config);
    }
}
