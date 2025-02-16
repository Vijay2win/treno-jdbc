package com.jpmc.hiveql;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import com.google.inject.Inject;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.aggregation.*;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.*;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.TemporaryTables.generateTemporaryTableName;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.getWriteBatchSize;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.isNonTransactionalInsert;
import static io.trino.plugin.jdbc.StandardColumnMappings.*;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.joining;

/**
 * TODO rewrite this. Reference: https://trino.io/docs/current/develop/connectors.html
 */
public class HiveJDBCClient extends BaseJdbcClient {
    static final Type TRINO_PAGE_SINK_ID_COLUMN_TYPE = BigintType.BIGINT;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    @Inject
    public HiveJDBCClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier) {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, false);

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .build();

        // TODO figure out the right setting for databricks
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, false))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(HiveJDBCClient::decimalTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
//                        .add(new ImplementAvgBigint())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .add(new ImplementCovarianceSamp())
                        .add(new ImplementCovariancePop())
                        .add(new ImplementCorr())
                        .add(new ImplementRegrIntercept())
                        .add(new ImplementRegrSlope())
                        .build());
    }

    @Override
    protected void dropTable(ConnectorSession session, RemoteTableName remoteTableName, boolean temporaryTable) {
        String sql = "DROP TABLE " + (remoteTableName.getSchemaName().get() + "." + remoteTableName.getTableName());
        execute(session, sql);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle) {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        switch (typeHandle.jdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());
            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());
            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.FLOAT:
            case Types.REAL:
                return Optional.of(realColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = typeHandle.requiredDecimalDigits();
                int precision = typeHandle.requiredColumnSize() + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
            case Types.NCHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.requiredColumnSize(), false));

            case Types.VARCHAR:
                int columnSize = typeHandle.requiredColumnSize();
                if (columnSize == -1) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType(), true));
                }
                return Optional.of(defaultVarcharColumnMapping(columnSize, true));

            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.requiredColumnSize(), false));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
//                return Optional.of(dateColumnMappingUsingSqlDate());
                return Optional.of(timeColumnMappingUsingSqlTime());

            case Types.TIME:
                // TODO Consider using `StandardColumnMappings.timeColumnMapping`
                return Optional.of(timeColumnMappingUsingSqlTime());

//            case Types.TIMESTAMP:
//                // TODO: use `StandardColumnMappings.timestampColumnMapping` when https://issues.apache.org/jira/browse/CALCITE-1630 gets resolved
//                return Optional.of(timestampColumnMappingUsingSqlTimestampWithFullPushdown(TIMESTAMP_MILLIS));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    @Override
    protected JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String targetTableName, Optional<ColumnMetadata> pageSinkIdColumn)
            throws SQLException {
        SchemaTableName schemaTableName = tableMetadata.getTable();

        ConnectorIdentity identity = session.getIdentity();
//        if (!getSchemaNames(session).contains(schemaTableName.getSchemaName())) {
//            throw new SchemaNotFoundException(schemaTableName.getSchemaName());
//        }

        try (Connection connection = connectionFactory.openConnection(session)) {
//                verify(connection.getAutoCommit());
            RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
            String remoteSchema = getIdentifierMapping().toRemoteSchemaName(remoteIdentifiers, identity, schemaTableName.getSchemaName());
            String remoteTable = getIdentifierMapping().toRemoteTableName(remoteIdentifiers, identity, remoteSchema, schemaTableName.getTableName());
            String remoteTargetTableName = getIdentifierMapping().toRemoteTableName(remoteIdentifiers, identity, remoteSchema, targetTableName);
            String catalog = connection.getCatalog();

            verifyTableName(connection.getMetaData(), remoteTargetTableName);

            return createTable(
                    session,
                    connection,
                    tableMetadata,
                    remoteIdentifiers,
                    catalog,
                    remoteSchema,
                    remoteTable,
                    remoteTargetTableName,
                    pageSinkIdColumn);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type) {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("BOOLEAN", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("int", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping("VARCHAR(" + charType.getLength() + ")", charWriteFunction(charType));
        }
        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "STRING";
            } else {
                dataType = "VARCHAR(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("VARBINARY", varbinaryWriteFunction());
        }
        if (type instanceof TimeType timeType) {
            return WriteMapping.longMapping(format("date(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
        }
        if (type instanceof DateType) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingSqlDate());
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    public static LongWriteFunction dateWriteFunctionUsingSqlDate()
    {
        return LongWriteFunction.of(Types.DATE, (statement, index, value) -> {
            // convert to midnight in default time zone
            long millis = DAYS.toMillis(value);
            statement.setString(index, "'" + new Date(DateTimeZone.UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis)) + "'");
        });
    }

    public static ColumnMapping bigintColumnMapping()
    {
        return ColumnMapping.longMapping(BIGINT, ResultSet::getLong, bigintWriteFunction());
    }

    public static LongWriteFunction bigintWriteFunction()
    {
        return LongWriteFunction.of(Types.BIGINT, PreparedStatement::setLong);
    }

//    @Override
//    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments) {
//        // TODO support complex ConnectorExpressions
//        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
//    }

//    @Override
//    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets) {
//        // Remote database can be case insensitive.
//        return preventTextualTypeAggregationPushdown(groupingSets);
//    }

    @Override
    protected void createSchema(ConnectorSession session, Connection connection, String remoteSchemaName) throws SQLException {
        execute(session, connection, "CREATE Database if not exists " + remoteSchemaName);
    }

    private static Optional<JdbcTypeHandle> decimalTypeHandle(DecimalType decimalType) {
        return Optional.of(new JdbcTypeHandle(
                Types.NUMERIC,
                Optional.of("NUMBER"),
                Optional.of(decimalType.getPrecision()),
                Optional.of(decimalType.getScale()),
                Optional.empty(),
                Optional.empty()));
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction() {
        return Optional.of((sql, limit)
                -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session) {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder) {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction() {
        return Optional.of(TopHiveNFunction.sqlStandard(this::quoted));
    }

    @FunctionalInterface
    public interface TopHiveNFunction extends TopNFunction
    {
        String apply(String query, List<JdbcSortItem> sortItems, long limit);

        static BaseJdbcClient.TopNFunction sqlStandard(Function<String, String> quote)
        {
            return (query, sortItems, limit) -> {
                String orderBy = sortItems.stream()
                        .map(sortItem -> {
                            String ordering = sortItem.sortOrder().isAscending() ? "ASC" : "DESC";
                            String nullsHandling = ""; // TODO handle this case
                            return format("%s %s %s", quote.apply(sortItem.column().getColumnName()), ordering, nullsHandling);
                        })
                        .collect(joining(", "));

                // TODO handle this better with row_num
                //return format("%s ORDER BY %s and row_num <= %s", query, orderBy, limit);
                return format("%s ORDER BY %s", query, orderBy, limit);
            };
        }
    }
    @Override
    public boolean isTopNGuaranteed(ConnectorSession session) {
        return false;
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException {
        // Empty remarks means that the table doesn't have a comment in Snowflake
        return Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata) {
        checkArgument(tableMetadata.getProperties().isEmpty(), "Unsupported table properties: %s", tableMetadata.getProperties());
        return ImmutableList.of(format("CREATE TABLE %s ( %s )",
                (remoteTableName.getSchemaName().get() + "." + remoteTableName.getTableName()),
                join(", ", columns)));
    }

    public String quoted(String name) {
        // column names doesnt need to be quoted.
        return name;
    }

    @Override
    protected String quoted(@jakarta.annotation.Nullable String catalog, @jakarta.annotation.Nullable String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            sb.append(catalog).append(".");
        }
        if (!isNullOrEmpty(schema)) {
            sb.append(schema).append(".");
        }
        sb.append(table);
        return sb.toString();
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment) {
        String sql = "COMMENT ON TABLE %s IS %s".formatted(
                handle.asPlainTable().getRemoteTableName(),
                quoted(comment.orElse("")));
        execute(session, sql);
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type) {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    private static SliceWriteFunction charWriteFunction(CharType charType) {
        return (statement, index, value) -> statement.setString(index, Chars.padSpaces(value, charType).toStringUtf8());
    }

    private static LongWriteFunction shortTimestampWriteFunction() {
        return (statement, index, value) -> statement.setString(index, StandardColumnMappings.fromTrinoTimestamp(value).toString());
    }

    @Override
    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName) {
        if (column.getComment() != null) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with column comment");
        }
        StringBuilder sb = new StringBuilder()
                .append(columnName)
                .append(" ")
                .append(toWriteMapping(session, column.getType()).getDataType());
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    @Override
    public Collection<String> listSchemas(Connection connection) {
        // for Clickhouse, we need to list catalogs instead of schemas
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (filterSchema(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        boolean hasPageSinkIdColumn = handle.getPageSinkIdColumnName().isPresent();
        checkArgument(handle.getColumnNames().size() == columnWriters.size(), "handle and columnWriters mismatch: %s, %s", handle, columnWriters);
        String sql = format(
                "INSERT INTO %s VALUES (%s%s)",
                (handle.getRemoteTableName().getSchemaName().orElse(null) + "." +
                        handle.getTemporaryTableName().orElseGet(() -> handle.getRemoteTableName().getTableName())),
//                The standard SQL syntax that allows the user to insert values into only some columns is not yet supported.
//                handle.getColumnNames().stream()
//                        .map(this::quoted)
//                        .collect(joining(", ")),
                hasPageSinkIdColumn ? ", " + quoted(handle.getPageSinkIdColumnName().get()) : "",
                columnWriters.stream()
                        .map(WriteFunction::getBindExpression)
                        .collect(joining(",")),
                hasPageSinkIdColumn ? ", ?" : "");
        return sql;
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        if (isNonTransactionalInsert(session)) {
            checkState(handle.getTemporaryTableName().isEmpty(), "Unexpected use of temporary table when non transactional inserts are enabled");
            return;
        }

        RemoteTableName temporaryTable = new RemoteTableName(
                handle.getRemoteTableName().getCatalogName(),
                handle.getRemoteTableName().getSchemaName(),
                handle.getTemporaryTableName().orElseThrow());

        // We conditionally create more than the one table, so keep a list of the tables that need to be dropped.
        Closer closer = Closer.create();
        closer.register(() -> dropTable(session, temporaryTable, true));

        try (Connection connection = getConnection(session, handle)) {
            verify(connection.getAutoCommit());
            String columns = handle.getColumnNames().stream()
                    .collect(joining(", "));

            String insertSql = format("INSERT INTO %s SELECT * FROM %s",
                    postProcessInsertTableNameClause(session, quoted(handle.getRemoteTableName())),
//                    columns,
                    quoted(temporaryTable));

            if (handle.getPageSinkIdColumnName().isPresent()) {
                RemoteTableName pageSinkTable = constructPageSinkIdsTable(session, connection, handle, pageSinkIds, closer);

                insertSql += format(" WHERE EXISTS (SELECT 1 FROM %s page_sink_table WHERE page_sink_table.%s = temp_table.%s)",
                        quoted(pageSinkTable),
                        handle.getPageSinkIdColumnName().get(),
                        handle.getPageSinkIdColumnName().get());
            }

            execute(session, connection, insertSql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        finally {
            try {
                closer.close();
            }
            catch (IOException e) {
                throw new TrinoException(JDBC_ERROR, e);
            }
        }
    }

    private RemoteTableName constructPageSinkIdsTable(ConnectorSession session, Connection connection, JdbcOutputTableHandle handle, Set<Long> pageSinkIds, Closer closer)
            throws SQLException
    {
        verify(handle.getPageSinkIdColumnName().isPresent(), "Output table handle's pageSinkIdColumn is empty");

        RemoteTableName pageSinkTable = new RemoteTableName(
                handle.getRemoteTableName().getCatalogName(),
                handle.getRemoteTableName().getSchemaName(),
                generateTemporaryTableName(session));

        int maxBatchSize = getWriteBatchSize(session);

        String pageSinkIdColumnName = handle.getPageSinkIdColumnName().get();

        String pageSinkTableSql = format("CREATE TABLE %s (%s)",
                quoted(pageSinkTable),
                getColumnDefinitionSql(session, new ColumnMetadata(pageSinkIdColumnName, TRINO_PAGE_SINK_ID_COLUMN_TYPE), pageSinkIdColumnName));
        String pageSinkInsertSql = format("INSERT INTO %s VALUES (?)", quoted(pageSinkTable));
        pageSinkInsertSql = queryModifier.apply(session, pageSinkInsertSql);
        LongWriteFunction pageSinkIdWriter = (LongWriteFunction) toWriteMapping(session, TRINO_PAGE_SINK_ID_COLUMN_TYPE).getWriteFunction();

        execute(session, connection, pageSinkTableSql);
        closer.register(() -> dropTable(session, pageSinkTable, true));

        try (PreparedStatement statement = connection.prepareStatement(pageSinkInsertSql)) {
            int batchSize = 0;
            for (Long pageSinkId : pageSinkIds) {
                pageSinkIdWriter.set(statement, 1, pageSinkId);

                statement.addBatch();
                batchSize++;

                if (batchSize >= maxBatchSize) {
                    statement.executeBatch();
                    batchSize = 0;
                }
            }
            if (batchSize > 0) {
                statement.executeBatch();
            }
        }

        return pageSinkTable;
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        execute(session, connection, format(
                "ALTER TABLE %s RENAME TO %s",
                remoteSchemaName + "." + remoteTableName,
                newRemoteSchemaName + "." + newRemoteTableName));
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, SchemaTableName schemaTableName, RemoteTableName remoteTableName) {
        RemoteTableName rtn;
        if (Strings.isNullOrEmpty(remoteTableName.getCatalogName().get())) {
            rtn = new RemoteTableName(Optional.empty(), remoteTableName.getSchemaName(), remoteTableName.getTableName());
        } else {
            rtn = new RemoteTableName(remoteTableName.getCatalogName(), remoteTableName.getSchemaName(), remoteTableName.getTableName());
        }
        return super.getColumns(session, schemaTableName, rtn);
    }

    public String quoted(RemoteTableName remoteTableName) {
        Optional<String> catalog = remoteTableName.getCatalogName();
        if (catalog.isPresent() && catalog.get().equals("")) {
            catalog = Optional.empty();
        }
        return append(catalog.orElse(null),
                remoteTableName.getSchemaName().orElse(null),
                remoteTableName.getTableName());
    }

    protected String append(@Nullable String catalog, @Nullable String schema, String table) {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            sb.append(catalog).append(".");
        }
        if (!isNullOrEmpty(schema)) {
            sb.append(schema).append(".");
        }
        sb.append(table);
        return sb.toString();
    }

    //    protected String quoted(@Nullable String catalog, @Nullable String schema, String table)
//    {
//        StringBuilder sb = new StringBuilder();
//        if (!isNullOrEmpty(schema)) {
//            sb.append(schema).append(".");
//        }
//        sb.append(table);
//        return sb.toString();
//    }
}
