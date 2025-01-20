package com.jpmc.hiveql;

import io.trino.testing.sql.SqlExecutor;

import static java.util.Objects.requireNonNull;

/**
 * Helper function like every other connectors tests
 */
public class DatabricksSqlExecutor
        implements SqlExecutor
{
    private final SqlExecutor delegate;

    public DatabricksSqlExecutor(SqlExecutor delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void execute(String sql)
    {
        // TODO if needed to be logged.
        delegate.execute(sql);
    }
}
