package com.jpmc.databricks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.aggregation.*;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.type.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
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
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Objects.requireNonNull;

/**
 * TODO rewrite this. Reference: https://trino.io/docs/current/develop/connectors.html
  */
public class DatabricksClient extends BaseJdbcClient {
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    @Inject
    public DatabricksClient(
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
                        .add(new ImplementSum(DatabricksClient::decimalTypeHandle))
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
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle) {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        String jdbcTypeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        jdbcTypeName = jdbcTypeName.toLowerCase(Locale.ENGLISH);
        int type = typeHandle.jdbcType();

        switch (type) {
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());
            // This is kept for synthetic columns generated by count() aggregation pushdown
            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            // In Snowflake all fixed-point numeric types are decimals. It always returns a DECIMAL type when JDBC_TREAT_DECIMAL_AS_INT is set to False.
            case Types.NUMERIC:
            case Types.DECIMAL: {
                int precision = typeHandle.requiredColumnSize();
                int scale = typeHandle.requiredDecimalDigits();
                if (precision > 38) {
                    break;
                }
                DecimalType decimalType = createDecimalType(precision, scale);
                return Optional.of(decimalColumnMapping(decimalType, UNNECESSARY));
            }
            case Types.VARCHAR:
                if (jdbcTypeName.equals("varchar")) {
                    return Optional.of(varcharColumnMapping(typeHandle.requiredColumnSize(), typeHandle.caseSensitivity()));
                }
                // Some other Snowflake types (ARRAY, VARIANT, GEOMETRY, etc.) are also mapped to Types.VARCHAR, but they're unsupported.
                break;
            case Types.BINARY:
                // Multiple Snowflake types are mapped into Types.BINARY
                if (jdbcTypeName.equals("binary")) {
                    return Optional.of(varbinaryColumnMapping());
                }
                // Some other Snowflake types (GEOMETRY in some cases, etc.) are also mapped to Types.BINARY, but they're unsupported.
                break;
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
        }
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type) {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("BOOLEAN", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("NUMBER(3, 0)", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("NUMBER(5, 0)", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("NUMBER(10, 0)", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("NUMBER(19, 0)", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("DOUBLE", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("DOUBLE", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = format("NUMBER(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
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
                dataType = "VARCHAR";
            } else {
                dataType = "VARCHAR(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("VARBINARY", varbinaryWriteFunction());
        }
        if (type instanceof TimeType timeType) {
            return WriteMapping.longMapping(format("TIME(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments) {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets) {
        // Remote database can be case insensitive.
        return preventTextualTypeAggregationPushdown(groupingSets);
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
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session) {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder) {
        for (JdbcSortItem sortItem : sortOrder) {
            Type sortItemType = sortItem.column().getColumnType();
            if (sortItemType instanceof CharType || sortItemType instanceof VarcharType) {
                // Remote database can be case insensitive.
                return false;
            }
        }
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction() {
        return Optional.of(TopNFunction.sqlStandard(this::quoted));
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session) {
        return true;
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
        return ImmutableList.of(format("CREATE TABLE %s (%s) COMMENT = %s", quoted(remoteTableName), join(", ", columns), snowflakeVarcharLiteral(tableMetadata.getComment().orElse(""))));
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment) {
        String sql = "COMMENT ON TABLE %s IS %s".formatted(
                quoted(handle.asPlainTable().getRemoteTableName()),
                snowflakeVarcharLiteral(comment.orElse("")));
        execute(session, sql);
    }

    private static String snowflakeVarcharLiteral(String value) {
        requireNonNull(value, "value is null");
        return "'" + value.replace("'", "''").replace("\\", "\\\\") + "'";
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type) {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength, Optional<CaseSensitivity> caseSensitivity) {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH ? createVarcharType(varcharLength) : createUnboundedVarcharType();
        return StandardColumnMappings.varcharColumnMapping(varcharType, caseSensitivity.orElse(CASE_INSENSITIVE) == CASE_SENSITIVE);
    }

    private static SliceWriteFunction charWriteFunction(CharType charType) {
        return (statement, index, value) -> statement.setString(index, Chars.padSpaces(value, charType).toStringUtf8());
    }

    private static LongWriteFunction shortTimestampWriteFunction() {
        return (statement, index, value) -> statement.setString(index, StandardColumnMappings.fromTrinoTimestamp(value).toString());
    }
}
