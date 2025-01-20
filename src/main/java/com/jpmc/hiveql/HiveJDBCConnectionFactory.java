package com.jpmc.hiveql;

import io.airlift.log.Logger;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForwardingConnection;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Main class for JDBC connector.
 */
public class HiveJDBCConnectionFactory implements ConnectionFactory {
    private static final Logger log = Logger.get(HiveJDBCConnectionFactory.class);
    private final DriverConnectionFactory factory;
    private final HiveJDBCConfig config;

    public HiveJDBCConnectionFactory(DriverConnectionFactory factory, HiveJDBCConfig config) {
        this.factory = factory;
        this.config = config;
    }

    @Override
    public Connection openConnection(ConnectorSession session) throws SQLException {
        return new ForwardingConnection() {
            private final Connection delegate = HiveJDBCConnectionFactory.this.factory.openConnection(session);

            @Override
            protected Connection delegate() {
                return delegate;
            }
        };
    }

    @Override
    public void close() throws SQLException {
        factory.close();
    }
}
