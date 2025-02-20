package com.clickhouse.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.clickhouse.client.ClickHouseParameterizedQuery;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.config.ClickHouseRenameMethod;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.data.ClickHouseRecordTransformer;
import com.clickhouse.client.ClickHouseSimpleResponse;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

@Deprecated
public class ClickHouseDatabaseMetaData extends JdbcWrapper implements DatabaseMetaData {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseDatabaseMetaData.class);

    private static final String DATABASE_NAME = "ClickHouse";
    private static final String DRIVER_NAME = DATABASE_NAME + " JDBC Driver";

    private static final String[] TABLE_TYPES = new String[] { "DICTIONARY", "LOG TABLE", "MEMORY TABLE",
            "REMOTE TABLE", "TABLE", "VIEW", "SYSTEM TABLE", "TEMPORARY TABLE" };

    private final ClickHouseConnection connection;

    protected ResultSet empty(String columns) throws SQLException {
        return fixed(columns, null);
    }

    protected ResultSet fixed(String columns, Object[][] values) throws SQLException {
        return new ClickHouseResultSet("", "", connection.createStatement(),
                ClickHouseSimpleResponse.of(connection.getConfig(), ClickHouseColumn.parse(columns), values));
    }

    protected ResultSet query(String sql) throws SQLException {
        return query(sql, null, false);
    }

    protected ResultSet query(String sql, boolean ignoreError) throws SQLException {
        return query(sql, null, ignoreError);
    }

    protected ResultSet query(String sql, ClickHouseRecordTransformer func) throws SQLException {
        return query(sql, func, false);
    }

    protected ResultSet query(String sql, ClickHouseRecordTransformer func, boolean ignoreError) throws SQLException {
        SQLException error = null;
        try (ClickHouseStatement stmt = connection.createStatement()) {
            stmt.setLargeMaxRows(0L);
            return new ClickHouseResultSet("", "", stmt,
                    // load everything into memory
                    ClickHouseSimpleResponse.of(stmt.getRequest()
                            .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                            .option(ClickHouseClientOption.RENAME_RESPONSE_COLUMN, ClickHouseRenameMethod.NONE)
                            .query(sql).executeAndWait(), func));
        } catch (Exception e) {
            error = SqlExceptionUtils.handle(e);
        }

        if (ignoreError) {
            return null;
        } else {
            throw error;
        }
    }

    public ClickHouseDatabaseMetaData(ClickHouseConnection connection) throws SQLException {
        this.connection = ClickHouseChecker.nonNull(connection, "Connection");
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return connection.getUri().toString();
    }

    @Override
    public String getUserName() throws SQLException {
        return connection.getCurrentUser();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return connection.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return DATABASE_NAME;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return connection.getServerVersion().toString();
    }

    @Override
    public String getDriverName() throws SQLException {
        return DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return DriverV1.driverVersionString;
    }

    @Override
    public int getDriverMajorVersion() {
        return DriverV1.driverVersion.getMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return DriverV1.driverVersion.getMinorVersion();
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "APPLY,ASOF,ATTACH,CLUSTER,DATABASE,DATABASES,DETACH,"
                + "DICTIONARY,DICTIONARIES,ILIKE,INF,LIMIT,LIVE,KILL,MATERIALIZED,"
                + "NAN,OFFSET,OPTIMIZE,OUTFILE,POLICY,PREWHERE,PROFILE,QUARTER,QUOTA,"
                + "RENAME,REPLACE,SAMPLE,SETTINGS,SHOW,TABLES,TIES,TOP,TOTALS,TRUNCATE,USE,WATCH,WEEK";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        // took from below URLs(not from system.functions):
        // https://clickhouse.com/docs/en/sql-reference/functions/arithmetic-functions/
        // https://clickhouse.com/docs/en/sql-reference/functions/math-functions/
        return "abs,acos,acosh,asin,asinh,atan,atan2,atanh,cbrt,cos,cosh,divide,e,erf,erfc,exp,exp10,exp2,gcd,hypot,intDiv,intDivOrZero,intExp10,intExp2,lcm,lgamma,ln,log,log10,log1p,log2,minus,modulo,moduloOrZero,multiply,negate,pi,plus,pow,power,sign,sin,sinh,sqrt,tan,tgamma";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        // took from below URLs(not from system.functions):
        // https://clickhouse.com/docs/en/sql-reference/functions/string-functions/
        // https://clickhouse.com/docs/en/sql-reference/functions/string-search-functions/
        // https://clickhouse.com/docs/en/sql-reference/functions/string-replace-functions/
        return "appendTrailingCharIfAbsent,base64Decode,base64Encode,char_length,CHAR_LENGTH,character_length,CHARACTER_LENGTH,concat,concatAssumeInjective,convertCharset,countMatches,countSubstrings,countSubstringsCaseInsensitive,countSubstringsCaseInsensitiveUTF8,CRC32,CRC32IEEE,CRC64,decodeXMLComponent,empty,encodeXMLComponent,endsWith,extract,extractAll,extractAllGroupsHorizontal,extractAllGroupsVertical,extractTextFromHTML ,format,ilike,isValidUTF8,lcase,leftPad,leftPadUTF8,length,lengthUTF8,like,locate,lower,lowerUTF8,match,mid,multiFuzzyMatchAllIndices,multiFuzzyMatchAny,multiFuzzyMatchAnyIndex,multiMatchAllIndices,multiMatchAny,multiMatchAnyIndex,multiSearchAllPositions,multiSearchAllPositionsUTF8,multiSearchAny,multiSearchFirstIndex,multiSearchFirstPosition,ngramDistance,ngramSearch,normalizedQueryHash,normalizeQuery,notEmpty,notLike,position,positionCaseInsensitive,positionCaseInsensitiveUTF8,positionUTF8,regexpQuoteMeta,repeat,replace,replaceAll,replaceOne,replaceRegexpAll,replaceRegexpOne,reverse,reverseUTF8,rightPad,rightPadUTF8,startsWith,substr,substring,substringUTF8,tokens,toValidUTF8,trim,trimBoth,trimLeft,trimRight,tryBase64Decode,ucase,upper,upperUTF8";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        // took from below URL(not from system.functions):
        // https://clickhouse.com/docs/en/sql-reference/functions/other-functions/
        return "bar,basename,blockNumber,blockSerializedSize,blockSize,buildId,byteSize,countDigits,currentDatabase,currentProfiles,currentRoles,currentUser,defaultProfiles,defaultRoles,defaultValueOfArgumentType,defaultValueOfTypeName,dumpColumnStructure,enabledProfiles,enabledRoles,errorCodeToName,filesystemAvailable,filesystemCapacity,filesystemFree,finalizeAggregation,formatReadableQuantity,formatReadableSize,formatReadableTimeDelta,FQDN,getMacro,getServerPort,getSetting,getSizeOfEnumType,greatest,hasColumnInTable,hostName,identity,ifNotFinite,ignore,indexHint,initializeAggregation,initialQueryID,isConstant,isDecimalOverflow,isFinite,isInfinite,isNaN,joinGet,least,MACNumToString,MACStringToNum,MACStringToOUI,materialize,modelEvaluate,neighbor,queryID,randomFixedString,randomPrintableASCII,randomString,randomStringUTF8,replicate,rowNumberInAllBlocks,rowNumberInBlock,runningAccumulate,runningConcurrency,runningDifference,runningDifferenceStartingWithFirstValue,shardCount ,shardNum,sleep,sleepEachRow,tcpPort,throwIf,toColumnTypeName,toTypeName,transform,uptime,version,visibleWidth";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        // took from below URL(not from system.functions):
        // https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/
        return "addDays,addHours,addMinutes,addMonths,addQuarters,addSeconds,addWeeks,addYears,date_add,date_diff,date_sub,date_trunc,dateName,formatDateTime,FROM_UNIXTIME,fromModifiedJulianDay,fromModifiedJulianDayOrNull,now,subtractDays,subtractHours,subtractMinutes,subtractMonths,subtractQuarters,subtractSeconds,subtractWeeks,subtractYears,timeSlot,timeSlots,timestamp_add,timestamp_sub,timeZone,timeZoneOf,timeZoneOffset,today,toDayOfMonth,toDayOfWeek,toDayOfYear,toHour,toISOWeek,toISOYear,toMinute,toModifiedJulianDay,toModifiedJulianDayOrNull,toMonday,toMonth,toQuarter,toRelativeDayNum,toRelativeHourNum,toRelativeMinuteNum,toRelativeMonthNum,toRelativeQuarterNum,toRelativeSecondNum,toRelativeWeekNum,toRelativeYearNum,toSecond,toStartOfDay,toStartOfFifteenMinutes,toStartOfFiveMinute,toStartOfHour,toStartOfInterval,toStartOfISOYear,toStartOfMinute,toStartOfMonth,toStartOfQuarter,toStartOfSecond,toStartOfTenMinutes,toStartOfWeek,toStartOfYear,toTime,toTimeZone,toUnixTimestamp,toWeek,toYear,toYearWeek,toYYYYMM,toYYYYMMDD,toYYYYMMDDhhmmss,yesterday";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        // TODO select { fn CONVERT({ts '2021-01-01 12:12:12'}, TIMESTAMP) }
        // select cast('2021-01-01 12:12:12' as DateTime)
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        // TODO select { fn CONVERT({ts '2021-01-01 12:12:12'}, TIMESTAMP) }
        // select cast('2021-01-01 12:12:12' as DateTime)
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        // TODO let's add this in 0.3.3
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return connection.getJdbcConfig().useSchema() ? JdbcConfig.TERM_DATABASE : JdbcConfig.TERM_SCHEMA;
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return connection.getJdbcConfig().useCatalog() ? JdbcConfig.TERM_DATABASE : JdbcConfig.TERM_CATALOG;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return connection.getJdbcConfig().useCatalog();
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return connection.getJdbcConfig().useSchema();
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return connection.getJdbcConfig().useSchema();
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return connection.getJdbcConfig().useSchema();
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return connection.getJdbcConfig().useSchema();
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return connection.getJdbcConfig().useSchema();
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return connection.getJdbcConfig().useCatalog();
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return connection.getJdbcConfig().useCatalog();
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return connection.getJdbcConfig().useCatalog();
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return connection.getJdbcConfig().useCatalog();
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return connection.getJdbcConfig().useCatalog();
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return true;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return connection.getJdbcConfig().isJdbcCompliant() ? Connection.TRANSACTION_REPEATABLE_READ
                : Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return connection.isTransactionSupported() || connection.getJdbcConfig().isJdbcCompliant();
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        if (Connection.TRANSACTION_NONE == level) {
            return true;
        } else if (Connection.TRANSACTION_READ_UNCOMMITTED != level && Connection.TRANSACTION_READ_COMMITTED != level
                && Connection.TRANSACTION_REPEATABLE_READ != level && Connection.TRANSACTION_SERIALIZABLE != level) {
            throw SqlExceptionUtils.clientError("Unknown isolation level: " + level);
        }

        return (connection.isTransactionSupported() && Connection.TRANSACTION_REPEATABLE_READ == level)
                || connection.getJdbcConfig().isJdbcCompliant();
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return connection.getJdbcConfig().isJdbcCompliant();
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return empty("PROCEDURE_CAT Nullable(String), PROCEDURE_SCHEM Nullable(String), "
                + "RESERVED1 Nullable(String), RESERVED2 Nullable(String), RESERVED3 Nullable(String), "
                + "PROCEDURE_NAME String, REMARKS String, PROCEDURE_TYPE Int16, SPECIFIC_NAME String");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        return empty("PROCEDURE_CAT Nullable(String), PROCEDURE_SCHEM Nullable(String), "
                + "PROCEDURE_NAME String, COLUMN_NAME String, COLUMN_TYPE Int16, "
                + "DATA_TYPE Int32, TYPE_NAME String, PRECISION Int32, LENGTH Int32, "
                + "SCALE Int16, RADIX Int16, NULLABLE Int16, REMARKS String, "
                + "COLUMN_DEF Nullable(String), SQL_DATA_TYPE Int32, SQL_DATETIME_SUB Int32, "
                + "CHAR_OCTET_LENGTH Int32, ORDINAL_POSITION Int32, IS_NULLABLE String, SPECIFIC_NAME String");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        StringBuilder builder = new StringBuilder();
        if (types == null || types.length == 0) {
            types = TABLE_TYPES;
        }
        for (String type : types) {
            builder.append('\'').append(ClickHouseUtils.escape(type, '\'')).append('\'').append(',');
        }
        builder.setLength(builder.length() - 1);

        String databasePattern = connection.getJdbcConfig().useCatalog() ? catalog : schemaPattern;
        List<String> databases = new LinkedList<>();
        if (ClickHouseChecker.isNullOrEmpty(databasePattern)) {
            try (ResultSet rs = query("select name from system.databases order by name")) {
                while (rs.next()) {
                    databases.add(rs.getString(1));
                }
            } catch (Exception e) {
                // ignore
            } finally {
                if (databases.isEmpty()) {
                    databases.add("%");
                }
            }
        } else {
            databases.add(databasePattern);
        }

        List<ResultSet> results = new ArrayList<>(databases.size());
        String commentColumn = connection.getServerVersion().check("[21.6,)") ? "t.comment" : "''";
        String catalogColumn = ClickHouseValues.NULL_EXPR;
        String schemaColumn = catalogColumn;
        if (connection.getJdbcConfig().useCatalog()) {
            catalogColumn = "t.database";
        } else {
            schemaColumn = "t.database";
        }
        for (String database : databases) {
            Map<String, String> params = new HashMap<>();
            params.put(JdbcConfig.TERM_COMMENT, commentColumn);
            params.put(JdbcConfig.TERM_CATALOG, catalogColumn);
            params.put(JdbcConfig.TERM_SCHEMA, schemaColumn);
            params.put(JdbcConfig.TERM_DATABASE, ClickHouseValues.convertToQuotedString(database));
            params.put(JdbcConfig.TERM_TABLE, ClickHouseChecker.isNullOrEmpty(tableNamePattern) ? "'%'"
                    : ClickHouseValues.convertToQuotedString(tableNamePattern));
            params.put("types", builder.toString());
            String sql = ClickHouseParameterizedQuery
                    .apply("select " +
                            ":catalog as TABLE_CAT, " +
                            ":schema as TABLE_SCHEM, " +
                            "t.name as TABLE_NAME, "
                            + "case when t.engine like '%Log' then 'LOG TABLE' "
                            + "when t.engine in ('Buffer', 'Memory', 'Set') then 'MEMORY TABLE' "
                            + "when t.is_temporary != 0 then 'TEMPORARY TABLE' "
                            + "when t.engine like '%View' then 'VIEW' when t.engine = 'Dictionary' then 'DICTIONARY' "
                            + "when t.engine like 'Async%' or t.engine like 'System%' then 'SYSTEM TABLE' "
                            + "when empty(t.data_paths) then 'REMOTE TABLE' else 'TABLE' end as TABLE_TYPE, "
                            + ":comment as REMARKS, " +
                            "null as TYPE_CAT, " +
                            "d.engine as TYPE_SCHEM, "
                            + "t.engine as TYPE_NAME, " +
                            "null as SELF_REFERENCING_COL_NAME, " +
                            "null as REF_GENERATION\n"
                            + "from system.tables t inner join system.databases d on t.database = d.name\n"
                            + "where t.database like :database and t.name like :table and TABLE_TYPE in (:types) "
                            + "order by t.database, t.name", params);
            results.add(query(sql, true));
        }
        return new CombinedResultSet(results);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        if (!connection.getJdbcConfig().useCatalog()) {
            return empty("TABLE_CAT String");
        }

        ResultSet rs = query("select name as TABLE_CAT from system.databases order by name");
        if (!connection.getJdbcConfig().isExternalDatabaseSupported()) {
            return rs;
        }
        return new CombinedResultSet(
                rs,
                query("select concat('jdbc(''', name, ''')') as TABLE_CAT from jdbc('', 'SHOW DATASOURCES') order by name",
                        true));
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        // "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY",
        // "ALIAS", "SYNONYM".
        int len = TABLE_TYPES.length;
        Object[][] rows = new Object[len][];
        for (int i = 0; i < len; i++) {
            rows[i] = new Object[] { TABLE_TYPES[i] };
        }
        return fixed("TABLE_TYPE String", rows);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        Map<String, String> params = new HashMap<>();
        params.put(JdbcConfig.TERM_COMMENT,
                connection.getServerVersion().check("[18.16,)") ? JdbcConfig.TERM_COMMENT : "''");
        if (connection.getJdbcConfig().useCatalog()) {
            params.put(JdbcConfig.TERM_CATALOG, JdbcConfig.TERM_DATABASE);
            params.put(JdbcConfig.TERM_SCHEMA, ClickHouseValues.NULL_EXPR);
        } else {
            params.put(JdbcConfig.TERM_CATALOG, ClickHouseValues.NULL_EXPR);
            params.put(JdbcConfig.TERM_SCHEMA, JdbcConfig.TERM_DATABASE);
        }
        String databasePattern = connection.getJdbcConfig().useCatalog() ? catalog : schemaPattern;
        params.put(JdbcConfig.TERM_DATABASE, ClickHouseChecker.isNullOrEmpty(databasePattern) ? "'%'"
                : ClickHouseValues.convertToQuotedString(databasePattern));
        params.put(JdbcConfig.TERM_TABLE, ClickHouseChecker.isNullOrEmpty(tableNamePattern) ? "'%'"
                : ClickHouseValues.convertToQuotedString(tableNamePattern));
        params.put("column", ClickHouseChecker.isNullOrEmpty(columnNamePattern) ? "'%'"
                : ClickHouseValues.convertToQuotedString(columnNamePattern));
        params.put("defaultNullable", String.valueOf(DatabaseMetaData.typeNullable));
        params.put("defaultNonNull", String.valueOf(DatabaseMetaData.typeNoNulls));
        params.put("defaultType", String.valueOf(Types.OTHER));
        String sql = ClickHouseParameterizedQuery
                .apply("select :catalog as TABLE_CAT, " +
                        ":schema as TABLE_SCHEM, " +
                        "table as TABLE_NAME, " +
                        "name as COLUMN_NAME, " +
                        "toInt32(:defaultType) as DATA_TYPE, " +
                        "type as TYPE_NAME, " +
                        "toInt32(0) as COLUMN_SIZE, "
                        + "0 as BUFFER_LENGTH, " +
                        "cast(null as Nullable(Int32)) as DECIMAL_DIGITS, " +
                        "10 as NUM_PREC_RADIX, "
                        + "toInt32(position(type, 'Nullable(') >= 1 ? :defaultNullable : :defaultNonNull) as NULLABLE, " +
                        ":comment as REMARKS, " +
                        "default_expression as COLUMN_DEF, "
                        + "0 as SQL_DATA_TYPE, " +
                        "0 as SQL_DATETIME_SUB, " +
                        "cast(null as Nullable(Int32)) as CHAR_OCTET_LENGTH, " +
                        "position as ORDINAL_POSITION, "
                        + "position(type, 'Nullable(') >= 1 ? 'YES' : 'NO' as IS_NULLABLE, " +
                        "null as SCOPE_CATALOG, null as SCOPE_SCHEMA, " +
                        "null as SCOPE_TABLE, " +
                        "null as SOURCE_DATA_TYPE, " +
                        "'NO' as IS_AUTOINCREMENT, " +
                        "'NO' as IS_GENERATEDCOLUMN " +
                        " FROM system.columns WHERE database LIKE :database and table LIKE :table AND name LIKE :column", params);
        return query(sql, (i, r) -> {
            String typeName = r.getValue("TYPE_NAME").asString();
            try {
                ClickHouseColumn column = ClickHouseColumn.of("", typeName);
                r.getValue("DATA_TYPE")
                        .update(connection.getJdbcTypeMapping().toSqlType(column, connection.getTypeMap()));
                r.getValue("COLUMN_SIZE").update(
                        column.getPrecision() > 0 ? column.getPrecision() : column.getDataType().getByteLength());
                if (column.isNullable()) {
                    r.getValue("NULLABLE").update(DatabaseMetaData.typeNullable);
                    r.getValue("IS_NULLABLE").update("YES");
                } else {
                    r.getValue("NULLABLE").update(DatabaseMetaData.typeNoNulls);
                    r.getValue("IS_NULLABLE").update("NO");
                }

                if (column.getDataType() == ClickHouseDataType.FixedString) {
                    r.getValue("CHAR_OCTET_LENGTH").update(column.getPrecision());
                }

                Class<?> clazz = column.getObjectClass(connection.getConfig());
                if (column.getScale() > 0 || Number.class.isAssignableFrom(clazz) || Date.class.isAssignableFrom(clazz)
                        || Temporal.class.isAssignableFrom(clazz)) {
                    r.getValue("DECIMAL_DIGITS").update(column.getScale());
                } else {
                    r.getValue("DECIMAL_DIGITS").resetToNullOrEmpty();
                }
            } catch (Exception e) {
                log.warn("Failed to read column: %s", typeName, e);
            }
        });
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        return empty("TABLE_CAT Nullable(String), TABLE_SCHEM Nullable(String), TABLE_NAME String, "
                + "COLUMN_NAME String, GRANTOR Nullable(String), GRANTEE String, PRIVILEGE String, "
                + "IS_GRANTABLE Nullable(String)");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return empty("TABLE_CAT Nullable(String), TABLE_SCHEM Nullable(String), TABLE_NAME String, "
                + "GRANTOR Nullable(String), GRANTEE String, PRIVILEGE String, IS_GRANTABLE Nullable(String)");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        return getVersionColumns(catalog, schema, table);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return empty("SCOPE Int16, COLUMN_NAME String, DATA_TYPE Int32, TYPE_NAME String, "
                + "COLUMN_SIZE Int32, BUFFER_LENGTH Int32, DECIMAL_DIGITS Int16, PSEUDO_COLUMN Int16");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return empty("TABLE_CAT Nullable(String), TABLE_SCHEM Nullable(String), TABLE_NAME String, "
                + "COLUMN_NAME String, KEY_SEQ Int16, PK_NAME String");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return empty("PKTABLE_CAT Nullable(String), PKTABLE_SCHEM Nullable(String), PKTABLE_NAME String, "
                + "PKCOLUMN_NAME String, FKTABLE_CAT Nullable(String), FKTABLE_SCHEM Nullable(String), "
                + "FKTABLE_NAME String, FKCOLUMN_NAME String, KEY_SEQ Int16, UPDATE_RULE Int16, "
                + "DELETE_RULE Int16, FK_NAME Nullable(String), PK_NAME Nullable(String), DEFERRABILITY Int16");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return getImportedKeys(catalog, schema, table);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return empty("PKTABLE_CAT Nullable(String), PKTABLE_SCHEM Nullable(String), PKTABLE_NAME String, "
                + "PKCOLUMN_NAME String, FKTABLE_CAT Nullable(String), FKTABLE_SCHEM Nullable(String), "
                + "FKTABLE_NAME String, FKCOLUMN_NAME String, KEY_SEQ Int16, UPDATE_RULE Int16, "
                + "DELETE_RULE Int16, FK_NAME Nullable(String), PK_NAME Nullable(String), DEFERRABILITY Int16");
    }

    private Object[] toTypeRow(String typeName, String aliasTo) throws SQLException {
        ClickHouseDataType type;
        try {
            type = ClickHouseDataType.of(typeName);
        } catch (Exception e) {
            if (aliasTo == null || aliasTo.isEmpty()) {
                return new Object[0];
            }
            try {
                type = ClickHouseDataType.of(aliasTo);
            } catch (Exception ex) {
                return new Object[0];
            }
        }

        String prefix = "";
        String suffix = "";
        String params = "";
        int nullable = DatabaseMetaData.typeNullable;
        int searchable = type == ClickHouseDataType.FixedString || type == ClickHouseDataType.String
                ? DatabaseMetaData.typeSearchable
                : DatabaseMetaData.typePredBasic;
        int money = 0;
        switch (type) {
            case Date:
            case Date32:
            case DateTime:
            case DateTime32:
            case DateTime64:
            case Enum8:
            case Enum16:
            case String:
            case FixedString:
            case UUID:
                prefix = "'";
                suffix = "'";
                break;
            case Array:
            case Nested:
            case Ring:
            case Polygon:
            case MultiPolygon:
                prefix = "[";
                suffix = "]";
                nullable = DatabaseMetaData.typeNoNulls;
                break;
            case AggregateFunction:
            case Tuple:
            case Point:
                prefix = "(";
                suffix = ")";
                nullable = DatabaseMetaData.typeNoNulls;
                break;
            case Map:
                prefix = "{";
                suffix = "}";
                nullable = DatabaseMetaData.typeNoNulls;
                break;
            default:
                break;
        }
        return new Object[] { typeName,
                connection.getJdbcTypeMapping().toSqlType(ClickHouseColumn.of("", type, false, false, new String[0]),
                        connection.getTypeMap()),
                type.getMaxPrecision(), prefix, suffix, params, nullable, type.isCaseSensitive() ? 1 : 0, searchable,
                type.getMaxPrecision() > 0 && !type.isSigned() ? 1 : 0, money, 0,
                aliasTo == null || aliasTo.isEmpty() ? type.name() : aliasTo, type.getMinScale(), type.getMaxScale(), 0,
                0, 10 };
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        List<Object[]> list = new ArrayList<>();
        try (ResultSet rs = query("select name, alias_to from system.data_type_families order by name")) {
            while (rs.next()) {
                Object[] row = toTypeRow(rs.getString(1), rs.getString(2));
                if (row.length > 0) {
                    list.add(row);
                }
            }
        }

        return fixed("TYPE_NAME String, DATA_TYPE Int32, PRECISION Int32, "
                + "LITERAL_PREFIX Nullable(String), LITERAL_SUFFIX Nullable(String), CREATE_PARAMS Nullable(String), "
                + "NULLABLE Int16, CASE_SENSITIVE UInt8, SEARCHABLE Int16, UNSIGNED_ATTRIBUTE UInt8, "
                + "FIXED_PREC_SCALE UInt8, AUTO_INCREMENT UInt8, LOCAL_TYPE_NAME Nullable(String), "
                + "MINIMUM_SCALE Int16, MAXIMUM_SCALE Int16, SQL_DATA_TYPE Int32, SQL_DATETIME_SUB Int32, "
                + "NUM_PREC_RADIX Int32", list.toArray(new Object[0][]));
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        Map<String, String> params = new HashMap<>();
        if (connection.getJdbcConfig().useCatalog()) {
            params.put(JdbcConfig.TERM_CATALOG, JdbcConfig.TERM_DATABASE);
            params.put(JdbcConfig.TERM_SCHEMA, ClickHouseValues.NULL_EXPR);
        } else {
            params.put(JdbcConfig.TERM_CATALOG, ClickHouseValues.NULL_EXPR);
            params.put(JdbcConfig.TERM_SCHEMA, JdbcConfig.TERM_DATABASE);
        }
        params.put(JdbcConfig.TERM_DATABASE,
                ClickHouseChecker.isNullOrEmpty(schema) ? "'%'" : ClickHouseValues.convertToQuotedString(schema));
        params.put(JdbcConfig.TERM_TABLE,
                ClickHouseChecker.isNullOrEmpty(table) ? "'%'" : ClickHouseValues.convertToQuotedString(table));
        params.put("statIndex", String.valueOf(DatabaseMetaData.tableIndexStatistic));
        params.put("otherIndex", String.valueOf(DatabaseMetaData.tableIndexOther));
        return new CombinedResultSet(
                empty("TABLE_CAT Nullable(String), TABLE_SCHEM Nullable(String), TABLE_NAME String, "
                        + "NON_UNIQUE UInt8, INDEX_QUALIFIER Nullable(String), INDEX_NAME Nullable(String), "
                        + "TYPE Int16, ORDINAL_POSITION Int16, COLUMN_NAME Nullable(String), ASC_OR_DESC Nullable(String), "
                        + "CARDINALITY Int64, PAGES Int64, FILTER_CONDITION Nullable(String)"),
                query(ClickHouseParameterizedQuery.apply(
                        "select :catalog as TABLE_CAT, :schema as TABLE_SCHEM, table as TABLE_NAME, toUInt8(0) as NON_UNIQUE, "
                                + "null as INDEX_QUALIFIER, null as INDEX_NAME, toInt16(:statIndex) as TYPE, "
                                + "toInt16(0) as ORDINAL_POSITION, null as COLUMN_NAME, null as ASC_OR_DESC, "
                                + "sum(rows) as CARDINALITY, uniqExact(name) as PAGES, null as FILTER_CONDITION from system.parts "
                                + "where active = 1 and database like :database and table like :table group by database, table",
                        params), true),
                query(ClickHouseParameterizedQuery.apply(
                        "select :catalog as TABLE_CAT, :schema as TABLE_SCHEM, table as TABLE_NAME, toUInt8(1) as NON_UNIQUE, "
                                + "type as INDEX_QUALIFIER, name as INDEX_NAME, toInt16(:otherIndex) as TYPE, "
                                + "toInt16(1) as ORDINAL_POSITION, expr as COLUMN_NAME, null as ASC_OR_DESC, "
                                + "0 as CARDINALITY, 0 as PAGES, null as FILTER_CONDITION "
                                + "from system.data_skipping_indices where database like :database and table like :table",
                        params), true),
                query(ClickHouseParameterizedQuery.apply(
                        "select :catalog as TABLE_CAT, :schema as TABLE_SCHEM, table as TABLE_NAME, toUInt8(1) as NON_UNIQUE, "
                                + "null as INDEX_QUALIFIER, name as INDEX_NAME, toInt16(:otherIndex) as TYPE, "
                                + "column_position as ORDINAL_POSITION, column as COLUMN_NAME, null as ASC_OR_DESC, "
                                + "sum(rows) as CARDINALITY, uniqExact(partition) as PAGES, null as FILTER_CONDITION "
                                + "from system.projection_parts_columns where active = 1 and database like :database and table like :table "
                                + "group by database, table, name, column, column_position "
                                + "order by database, table, name, column_position",
                        params), true));
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY == type;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        return empty("TYPE_CAT Nullable(String), TYPE_SCHEM Nullable(String), TYPE_NAME String, "
                + "CLASS_NAME String, DATA_TYPE Int32, REMARKS String, BASE_TYPE Int16");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return empty("TYPE_CAT Nullable(String), TYPE_SCHEM Nullable(String), TYPE_NAME String, "
                + "SUPERTYPE_CAT Nullable(String), SUPERTYPE_SCHEM Nullable(String), SUPERTYPE_NAME String");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return empty(
                "TABLE_CAT Nullable(String), TABLE_SCHEM Nullable(String), TABLE_NAME String, SUPERTABLE_NAME String");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
        return empty("TYPE_CAT Nullable(String), TYPE_SCHEM Nullable(String), TYPE_NAME String, "
                + "ATTR_NAME String, DATA_TYPE Int32, ATTR_TYPE_NAME String, ATTR_SIZE Int32, "
                + "DECIMAL_DIGITS Int32, NUM_PREC_RADIX Int32, NULLABLE Int32, REMARKS Nullable(String), "
                + "ATTR_DEF Nullable(String), SQL_DATA_TYPE Int32, SQL_DATETIME_SUB Int32, "
                + "CHAR_OCTET_LENGTH Int32, ORDINAL_POSITION Int32, IS_NULLABLE String, "
                + "SCOPE_CATALOG String, SCOPE_SCHEMA String, SCOPE_TABLE String, SOURCE_DATA_TYPE Int16");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return connection.getServerVersion().getMajorVersion();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return connection.getServerVersion().getMinorVersion();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return DriverV1.specVersion.getMajorVersion();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return DriverV1.specVersion.getMinorVersion();
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        if (!connection.getJdbcConfig().useSchema()) {
            return empty("TABLE_SCHEM String, TABLE_CATALOG Nullable(String)");
        }

        Map<String, String> params = Collections.singletonMap("pattern",
                ClickHouseChecker.isNullOrEmpty(schemaPattern) ? "'%'"
                        : ClickHouseValues.convertToQuotedString(schemaPattern));
        ResultSet rs = query(ClickHouseParameterizedQuery.apply("select name as TABLE_SCHEM, null as TABLE_CATALOG "
                + "from system.databases where name like :pattern order by name", params));
        if (!connection.getJdbcConfig().isExternalDatabaseSupported()) {
            return rs;
        }

        return new CombinedResultSet(
                rs,
                query(ClickHouseParameterizedQuery.apply(
                        "select concat('jdbc(''', name, ''')') as TABLE_SCHEM, null as TABLE_CATALOG "
                                + "from jdbc('', 'SHOW DATASOURCES') where TABLE_SCHEM like :pattern order by name",
                        params), true));
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        ClickHouseParameterizedQuery q = ClickHouseParameterizedQuery
                .of(connection.getConfig(),
                        "select :name as NAME, toInt32(0) as MAX_LEN, :default as DEFAULT_VALUE, :desc as DESCRIPTION");
        StringBuilder builder = new StringBuilder();
        q.apply(builder, ClickHouseValues.convertToQuotedString(ClickHouseConnection.PROP_APPLICATION_NAME),
                ClickHouseValues
                        .convertToQuotedString(connection.getClientInfo(ClickHouseConnection.PROP_APPLICATION_NAME)),
                ClickHouseValues.convertToQuotedString("Application name"));
        builder.append(" union all ");
        q.apply(builder, ClickHouseValues.convertToQuotedString(ClickHouseConnection.PROP_CUSTOM_HTTP_HEADERS),
                ClickHouseValues
                        .convertToQuotedString(connection.getClientInfo(ClickHouseConnection.PROP_CUSTOM_HTTP_HEADERS)),
                ClickHouseValues.convertToQuotedString("Custom HTTP headers"));
        builder.append(" union all ");
        q.apply(builder, ClickHouseValues.convertToQuotedString(ClickHouseConnection.PROP_CUSTOM_HTTP_PARAMS),
                ClickHouseValues
                        .convertToQuotedString(connection.getClientInfo(ClickHouseConnection.PROP_CUSTOM_HTTP_PARAMS)),
                ClickHouseValues.convertToQuotedString("Customer HTTP query parameters"));
        return query(builder.toString());
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        Map<String, String> params = new HashMap<>();
        String databasePattern = connection.getJdbcConfig().useCatalog() ? catalog : schemaPattern;
        boolean systemDatabase = ClickHouseChecker.isNullOrEmpty(databasePattern);
        if (!systemDatabase) {
            String databasePatternLower = databasePattern.toLowerCase(Locale.ROOT);
            systemDatabase = "system".contains(databasePatternLower)
                    || "information_schema".contains(databasePatternLower);
        }
        params.put("filter", systemDatabase ? "1" : "0");
        params.put("pattern", ClickHouseChecker.isNullOrEmpty(functionNamePattern) ? "'%'"
                : ClickHouseValues.convertToQuotedString(functionNamePattern));

        String sql = ClickHouseParameterizedQuery.apply(
                "select * from (select null as FUNCTION_CAT, 'system' as FUNCTION_SCHEM, name as FUNCTION_NAME,\n"
                        + "concat('case-', case_insensitive ? 'in' : '', 'sensitive function', is_aggregate ? ' for aggregation' : '') as REMARKS,"
                        + "1 as FUNCTION_TYPE, name as SPECIFIC_NAME from system.functions where name like :pattern union all\n"
                        + "select null as FUNCTION_CAT, 'system' as FUNCTION_SCHEM, name as FUNCTION_NAME,\n"
                        + "'case-sensitive table function' as REMARKS, 2 as FUNCTION_TYPE, name as SPECIFIC_NAME from system.table_functions\n"
                        + "where name not in (select name from system.functions) and name like :pattern) where :filter\n"
                        + "order by FUNCTION_CAT, FUNCTION_SCHEM, FUNCTION_NAME",
                params);
        return query(sql);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {
        return empty("FUNCTION_CAT Nullable(String), FUNCTION_SCHEM Nullable(String), FUNCTION_NAME String,"
                + "COLUMN_NAME String, COLUMN_TYPE Int16, DATA_TYPE Int32, TYPE_NAME String, PRECISION Int32,"
                + "LENGTH Int32, SCALE Int16, RADIX Int16, NULLABLE Int16, REMARKS String, CHAR_OCTET_LENGTH Int32,"
                + "ORDINAL_POSITION Int32, IS_NULLABLE String, SPECIFIC_NAME String");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        return empty("TABLE_CAT Nullable(String), TABLE_SCHEM Nullable(String), TABLE_NAME String, "
                + "COLUMN_NAME String, DATA_TYPE Int32, COLUMN_SIZE Int32, DECIMAL_DIGITS Int32, "
                + "NUM_PREC_RADIX Int32, COLUMN_USAGE String, REMARKS Nullable(String), "
                + "CHAR_OCTET_LENGTH Int32, IS_NULLABLE String");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }
}
