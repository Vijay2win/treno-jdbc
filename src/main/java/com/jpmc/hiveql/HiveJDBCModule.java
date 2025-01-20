package com.jpmc.hiveql;

import com.google.inject.Module;
import com.google.inject.*;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Main module configuration
 */
public class HiveJDBCModule implements Module {

    @Override
    public void configure(Binder binder) {
        configBinder(binder).bindConfig(HiveJDBCConfig.class);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(HiveJDBCClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        binder.install(new DecimalModule());
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Singleton
    @Provides
    @ForBaseJdbc
    public ConnectionFactory getConnectionFactory(HiveJDBCConfig config, CredentialProvider credentials, OpenTelemetry openTelemetry) {
        Properties properties = new Properties();
        return new HiveJDBCConnectionFactory(
                DriverConnectionFactory.builder(new DatabricksDriver(config), config.getConnectionUrl(), credentials)
                        .setCredentialPropertiesProvider(new OauthCredentialPropertiesProvider(credentials))
                        .setConnectionProperties(properties)
                        .build(), config);
    }
}
