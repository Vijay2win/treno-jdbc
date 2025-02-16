package com.jpmc.hiveql;

import org.apache.commons.dbcp2.DelegatingConnection;
import org.apache.commons.dbcp2.DelegatingPreparedStatement;

import java.sql.*;

public class HiveConnectionWithCatalog extends DelegatingConnection {
    private final Connection delegate;
    private final String url;

    public HiveConnectionWithCatalog(Connection delegate, String url) {
        super(delegate);
        this.delegate = delegate;
        this.url = url;
    }


    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        try {
            super.setReadOnly(readOnly);
        } catch (SQLException ex) {
            // HiveConnector doesnt support, ignored.
        }
    }

    @Override
    public void commit() throws SQLException {
        try {
            super.commit();
        } catch (SQLException ex) {
            // ignore this since we hive doesnt support commit.
            // TODO log it
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new DelegatingPreparedStatement(this, super.prepareStatement(sql)) {
            @Override
            public void addBatch() throws SQLException {
                try {
                    super.addBatch();
                } catch (SQLException ex) {
                    // HiveConnector doesnt support addBatch
                    super.execute();
                }
            }
        };
    }
}
