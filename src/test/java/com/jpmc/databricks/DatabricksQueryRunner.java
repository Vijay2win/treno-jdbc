package com.jpmc.databricks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

/**
 * This is query runner this will configure the connection and run a distributed query.
 */
public final class DatabricksQueryRunner {
    public static final String TPCH_SCHEMA = "tpch";

    public static Builder builder(TestingDatabricksServer server) {
        Builder builder = new Builder()
                .addConnectorProperty("connection-url", server.getJdbcUrl())
                .addConnectorProperty("http-path", "sql/protocolv1/o/3414251373208641/1227-183607-gazza0cf");
        return builder;
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder> {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder() {
            super(testSessionBuilder()
                    .setCatalog("databricks")
                    .setSchema(TPCH_SCHEMA)
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value) {
            this.connectorProperties.put(key, value);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables) {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build() throws Exception {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new DatabricksPlugin());
                queryRunner.createCatalog("databricks", "databricks", connectorProperties);
                queryRunner.execute("create schema IF NOT EXISTS " + TPCH_SCHEMA);
                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, initialTables);

                return queryRunner;
            } catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static void main(String[] args)
            throws Exception {
        QueryRunner queryRunner = builder(new TestingDatabricksServer())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(DatabricksQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
