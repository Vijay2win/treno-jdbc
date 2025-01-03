package com.jpmc.databricks;

import io.airlift.log.Logger;
import io.trino.FullConnectorSession;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForwardingConnection;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Main class for JDBC connector.
 */
public class DatabricksConnectionFactory implements ConnectionFactory {
    private static final Logger log = Logger.get(DatabricksConnectionFactory.class);
    private final DriverConnectionFactory factory;
    private final DatabricksConfig config;

    public DatabricksConnectionFactory(DriverConnectionFactory factory, DatabricksConfig config) {
        this.factory = factory;
        this.config = config;
    }

    @Override
    public Connection openConnection(ConnectorSession session) throws SQLException {
        return new ForwardingConnection() {
            private final Connection delegate = DatabricksConnectionFactory.this.factory.openConnection(session);
            private String catalog;

            @Override
            protected Connection delegate() throws SQLException {
                if (session instanceof FullConnectorSession) {
                    catalog = ((FullConnectorSession) session).getSession().getCatalog().orElse("");
                    delegate.setCatalog(catalog);
                }
                return delegate;
            }

            @Override
            public String getCatalog() throws SQLException {
                return catalog;
            }
        };
    }
}
