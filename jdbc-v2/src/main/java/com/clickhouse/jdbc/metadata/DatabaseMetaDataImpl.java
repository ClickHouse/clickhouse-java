package com.clickhouse.jdbc.metadata;

import com.clickhouse.client.api.sql.SQLUtils;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.ClientInfoProperties;
import com.clickhouse.jdbc.ConnectionImpl;
import com.clickhouse.jdbc.Driver;
import com.clickhouse.jdbc.DriverProperties;
import com.clickhouse.jdbc.JdbcV2Wrapper;
import com.clickhouse.jdbc.internal.DetachedResultSet;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.clickhouse.jdbc.internal.JdbcUtils;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DatabaseMetaDataImpl implements java.sql.DatabaseMetaData, JdbcV2Wrapper {
    private static final Logger log = LoggerFactory.getLogger(DatabaseMetaDataImpl.class);
    public static final String[] TABLE_TYPES = new String[] { "DICTIONARY", "LOG TABLE", "MEMORY TABLE",
            "REMOTE TABLE", "TABLE", "VIEW", "SYSTEM TABLE", "TEMPORARY TABLE" };

    ConnectionImpl connection;

    private boolean useCatalogs = false;
    private String catalogPlaceholder;

    private String jdbcUrl;

    /**
     * Creates an instance of DatabaseMetaData for the given connection.
     *
     *
     * @param connection - connection for which metadata is created
     * @param useCatalogs - if true then getCatalogs() will return non-empty list (not implemented yet)
     */
    public DatabaseMetaDataImpl(ConnectionImpl connection, boolean useCatalogs, String url) throws SQLFeatureNotSupportedException {
        if (useCatalogs) {
            throw new SQLFeatureNotSupportedException("Catalogs are not supported yet", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
        this.connection = connection;
        this.useCatalogs = useCatalogs;
        this.catalogPlaceholder = useCatalogs ? "'local' " : "''";
        this.jdbcUrl = url;
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
        return jdbcUrl;
    }

    @Override
    public String getUserName() throws SQLException {
        try {
            return connection.getClient().getUser();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false; // There is no way to detect if database is read only
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        // null sorted low by default - https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#sorting-of-special-values
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return !nullsAreSortedHigh(); // opposite of nullsAreSortedHigh
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        // null are sorted  https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#sorting-of-special-values
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "ClickHouse";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        try {
            return connection.getServerVersion();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public String getDriverName() throws SQLException {
        return "ClickHouse JDBC Driver";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return Driver.getLibraryVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return Driver.getDriverMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return Driver.getDriverMinorVersion();
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
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
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

    /**
     * Schema terms relates to a collection of data definitions, rules, stored procedures, etc.
     * ClickHouse has "database" as the closest term to schema.
     * @return - string "database"
     */
    @Override
    public String getSchemaTerm() {
        return connection.getJdbcConfig().getDriverProperty(DriverProperties.SCHEMA_TERM.getKey(), "schema");
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "function";
    }

    /**
     * Catalog term relates to the top level scale like clusters, something that joins multiple databases
     * Returns most close term to catalog in ClickHouse which is "cluster".
     * @return - string "cluster"
     */
    @Override
    public String getCatalogTerm() {
        return "cluster";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
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
        try {
            return connection.getTransactionIsolation();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        try {
            return level == connection.getTransactionIsolation();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
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
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        String sql = "SELECT " +
                "'' AS PROCEDURE_CAT, " +
                "'' AS PROCEDURE_SCHEM, " +
                "'' AS PROCEDURE_NAME, " +
                "0::Int16 AS RESERVED1, " +
                "0::Int16 AS RESERVED2, " +
                "0::Int16 AS RESERVED3, " +
                "'' AS REMARKS, " +
                "0::Int16 AS PROCEDURE_TYPE, " +
                "'' AS SPECIFIC_NAME " +
                "LIMIT 0";
        try {
            return connection.createStatement().executeQuery(sql);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        String sql = "SELECT " +
                "'' AS PROCEDURE_CAT, " +
                "'' AS PROCEDURE_SCHEM, " +
                "'' AS PROCEDURE_NAME, " +
                "'' AS COLUMN_NAME, " +
                "0::Int16 AS COLUMN_TYPE, " +
                "0::Int32 AS DATA_TYPE, " +
                "'' AS TYPE_NAME, " +
                "0::Int32 AS PRECISION, " +
                "0::Int32 AS LENGTH, " +
                "0::Int16 AS SCALE, " +
                "0::Int16 AS RADIX, " +
                "0::Int16 AS NULLABLE, " +
                "'' AS REMARKS, " +
                "'' AS COLUMN_DEF, " +
                "0::Int32 AS SQL_DATA_TYPE, " +
                "0::Int32 AS SQL_DATETIME_SUB, " +
                "0::Int32 AS CHAR_OCTET_LENGTH, " +
                "0::Int32 AS ORDINAL_POSITION, " +
                "'' AS IS_NULLABLE, " +
                "'' AS SPECIFIC_NAME " +
                "LIMIT 0";
        try {
            return connection.createStatement().executeQuery(sql);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    /**
     * Returns tables defined for a schema. Parameter {@code catalog} is ignored
     *
     * @return - ResultSet with information about tables
     * @throws SQLException
     */
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        log.debug("getTables: catalog={}, schemaPattern={}, tableNamePattern={}, types={}", catalog, schemaPattern, tableNamePattern, types);
        // TODO: when switch between catalog and schema is implemented, then TABLE_SCHEMA and TABLE_CAT should be populated accordingly
//        String commentColumn = connection.getServerVersion().check("[21.6,)") ? "t.comment" : "''";
        // TODO: handle useCatalogs == true and return schema catalog name
        if (types == null || types.length == 0) {
            types = TABLE_TYPES;
        }

        String sql = "SELECT " +
                 catalogPlaceholder + " AS TABLE_CAT, " +
                "t.database AS TABLE_SCHEM, " +
                "t.name AS TABLE_NAME, " +
                "CASE WHEN t.engine LIKE '%Log' THEN 'LOG TABLE' " +
                "WHEN t.engine in ('Buffer', 'Memory', 'Set') THEN 'MEMORY TABLE' " +
                "WHEN t.is_temporary != 0 THEN 'TEMPORARY TABLE' " +
                "WHEN t.engine like '%View' THEN 'VIEW'" +
                "WHEN t.engine = 'Dictionary' THEN 'DICTIONARY' " +
                "WHEN t.engine LIKE 'Async%' OR t.engine LIKE 'System%' THEN 'SYSTEM TABLE' " +
                "WHEN empty(t.data_paths) THEN 'REMOTE TABLE' " +
                "ELSE 'TABLE' END AS TABLE_TYPE, " +
                "t.comment AS REMARKS, " +
                "null::Nullable(String) AS TYPE_CAT, " + // no types catalog
                "d.engine AS TYPE_SCHEM, " + // no types schema
                "null::Nullable(String) AS TYPE_NAME, " + // vendor type name ?
                "null::Nullable(String) AS SELF_REFERENCING_COL_NAME, " +
                "null::Nullable(String) AS REF_GENERATION" +
                " FROM system.tables t" +
                " JOIN system.databases d ON system.tables.database = system.databases.name" +
                " WHERE t.database LIKE '" + (schemaPattern == null ? "%" : schemaPattern) + "'" +
                " AND t.name LIKE '" + (tableNamePattern == null ? "%" : tableNamePattern) + "'" +
                " AND TABLE_TYPE IN ('" + String.join("','", types) + "')";

        try {
            return connection.createStatement().executeQuery(sql);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    /**
     * Returns a ResultSet with the following columns:
     * TABLE_CAT - String - catalog name
     * TABLE_SCHEM - String - schema name
     * TABLE_NAME - String - table name
     * @return - ResultSet with the above columns
     * @throws SQLException - if an error occurs
     */
    @Override
    public ResultSet getSchemas() throws SQLException {
        // TODO: handle useCatalogs == true and return schema catalog name
        try {
            return connection.createStatement().executeQuery("SELECT name AS TABLE_SCHEM, " + catalogPlaceholder + " AS TABLE_CATALOG FROM system.databases ORDER BY name");
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    /**
     * The closes term to catalog in ClickHouse is "cluster".
     * Current implementation version doesn't support work with cluster and will always return
     * @return - ResultSet with one column TABLE_CAT and one row with value "local"
     * @throws SQLException - if an error occurs
     */
    @Override
    public ResultSet getCatalogs() throws SQLException {
        try {
            return connection.createStatement().executeQuery("SELECT 'local' AS TABLE_CAT "  + (useCatalogs ? "" : " WHERE 1 = 0"));
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    /**
     * Returns name of the ClickHouse table types as the broad category (rather than engine name).
     * @return - ResultSet with one column TABLE_TYPE
     * @throws SQLException - if an error occurs
     */
    @Override
    public ResultSet getTableTypes() throws SQLException {
        try {
            return connection.createStatement().executeQuery("SELECT arrayJoin(['" + String.join("','", TABLE_TYPES) + "']) AS TABLE_TYPE ORDER BY TABLE_TYPE");
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    @SuppressWarnings({"squid:S2095", "squid:S2077"})
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        // TODO: handle useCatalogs == true and return schema catalog name
        final String sql = "SELECT " +
                catalogPlaceholder + " AS TABLE_CAT, " +
                "database AS TABLE_SCHEM, " +
                "table AS TABLE_NAME, " +
                "name AS COLUMN_NAME, " +
                "toInt32(" + Types.OTHER + ") AS DATA_TYPE, " +
                "type AS TYPE_NAME, " +
                "toInt32(" + generateSqlTypeSizes("system.columns.type") + ") AS COLUMN_SIZE, " +
                "toInt32(0) AS BUFFER_LENGTH, " +
                "toInt32(IF (numeric_scale == 0, NULL, numeric_scale)) as DECIMAL_DIGITS,  " +
                "toInt32(numeric_precision_radix) AS NUM_PREC_RADIX, " +
                "toInt32(position(type, 'Nullable(') >= 1 ?" + java.sql.DatabaseMetaData.typeNullable + " : " + java.sql.DatabaseMetaData.typeNoNulls + ") as NULLABLE, " +
                "system.columns.comment AS REMARKS, " +
                "system.columns.default_expression AS COLUMN_DEF, " +
                "toInt32(0) AS SQL_DATA_TYPE, " +
                "toInt32(0) AS SQL_DATETIME_SUB, " +
                "character_octet_length::Nullable(Int32) AS CHAR_OCTET_LENGTH, " +
                "toInt32(system.columns.position) AS ORDINAL_POSITION, " +
                "position(upper(type), 'NULLABLE') >= 1 ? 'YES' : 'NO' AS IS_NULLABLE," +
                "NULL::Nullable(String) AS SCOPE_CATALOG, " +
                "NULL::Nullable(String) AS SCOPE_SCHEMA, " +
                "NULL::Nullable(String) AS SCOPE_TABLE, " +
                "NULL::Nullable(Int16) AS SOURCE_DATA_TYPE, " +
                "'NO' as IS_AUTOINCREMENT, " +
                "'NO' as IS_GENERATEDCOLUMN " +
                " FROM system.columns" +
                " WHERE database LIKE " + SQLUtils.enquoteLiteral(schemaPattern == null ? "%" : schemaPattern) +
                " AND table LIKE " + SQLUtils.enquoteLiteral(tableNamePattern == null ? "%" : tableNamePattern) +
                " AND name LIKE " + SQLUtils.enquoteLiteral(columnNamePattern == null ? "%" : columnNamePattern) +
                " ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION";
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            return DetachedResultSet.createFromResultSet(rs, connection.getDefaultCalendar(), GET_COLUMNS_RS_MUTATORS);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    private static String generateSqlTypeSizes(String columnName) {
        StringBuilder sql = new StringBuilder("multiIf(");
        sql.append("character_octet_length IS NOT NULL, character_octet_length, ");
        for (ClickHouseDataType type : ClickHouseDataType.values()) {
            if (type.getByteLength() > 0) {
                sql.append(columnName).append(" == '").append(type.name()).append("', ").append(type.getByteLength()).append(", ");
            }
        }
        sql.append("numeric_precision IS NOT NULL, numeric_precision, ");
        sql.append("0)");
        return sql.toString();
    }


    private static final Consumer<Map<String, Object>> DATA_TYPE_VALUE_FUNCTION = row -> {
        String typeName = (String) row.get("TYPE_NAME");
        SQLType type = JdbcUtils.CLICKHOUSE_TYPE_NAME_TO_SQL_TYPE_MAP.get(typeName);
        if (type == null) {
            try {
                type = JdbcUtils.convertToSqlType(ClickHouseDataType.valueOf(typeName));
            } catch (Exception e) {
                log.error("Failed to convert column data type to SQL type: {}", typeName, e);
                type = JDBCType.OTHER; // In case of error, return SQL type 0
            }
        }

        row.put("DATA_TYPE", type.getVendorTypeNumber());
    };

    private static final List<Consumer<Map<String, Object>>> GET_COLUMNS_RS_MUTATORS = Collections.singletonList(DATA_TYPE_VALUE_FUNCTION);

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        //Return an empty result set with the required columns
        try {
            return connection.createStatement().executeQuery("SELECT NULL::Nullable(String) AS TABLE_CAT, " +
                    "NULL::Nullable(String) AS TABLE_SCHEM, " +
                    "NULL::Nullable(String) AS TABLE_NAME, " +
                    "NULL::Nullable(String) AS COLUMN_NAME, " +
                    "NULL::Nullable(String) AS GRANTOR, " +
                    "NULL::Nullable(String) AS GRANTEE, " +
                    "NULL::Nullable(String) AS PRIVILEGE, " +
                    "NULL::Nullable(String) AS IS_GRANTABLE" +
                    " LIMIT 0");
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        //Return an empty result set with the required columns
        try {
            return connection.createStatement().executeQuery("SELECT NULL::Nullable(String) AS TABLE_CAT, " +
                    "NULL::Nullable(String) AS TABLE_SCHEM, " +
                    "NULL::Nullable(String) AS TABLE_NAME, " +
                    "NULL::Nullable(String) AS GRANTOR, " +
                    "NULL::Nullable(String) AS GRANTEE, " +
                    "NULL::Nullable(String) AS PRIVILEGE, " +
                    "NULL::Nullable(String) AS IS_GRANTABLE" +
                    " LIMIT 0");
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        //Return an empty result set with the required columns
        try {
            return connection.createStatement().executeQuery("SELECT NULL::Nullable(Int16) AS SCOPE, " +
                    "NULL::Nullable(String) AS COLUMN_NAME, " +
                    "NULL::Nullable(Int32) AS DATA_TYPE, " +
                    "NULL::Nullable(String) AS TYPE_NAME, " +
                    "NULL::Nullable(Int32) AS COLUMN_SIZE, " +
                    "NULL::Nullable(Int32) AS BUFFER_LENGTH, " +
                    "NULL::Nullable(Int16) AS DECIMAL_DIGITS, " +
                    "NULL::Nullable(Int16) AS PSEUDO_COLUMN" +
                    " LIMIT 0");
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        //Return an empty result set with the required columns
        try {
            return connection.createStatement().executeQuery("SELECT NULL::Nullable(Int16) AS SCOPE, " +
                    "NULL::Nullable(String) AS COLUMN_NAME, " +
                    "NULL::Nullable(Int32) AS DATA_TYPE, " +
                    "NULL::Nullable(String) AS TYPE_NAME, " +
                    "NULL::Nullable(Int32) AS COLUMN_SIZE, " +
                    "NULL::Nullable(Int32) AS BUFFER_LENGTH, " +
                    "NULL::Nullable(Int16) AS DECIMAL_DIGITS, " +
                    "NULL::Nullable(Int16) AS PSEUDO_COLUMN" +
                    " LIMIT 0");
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        try {
            String sql = "SELECT NULL::Nullable(String) AS TABLE_CAT, " +
                    "system.tables.database AS TABLE_SCHEM, " +
                    "system.tables.name AS TABLE_NAME, " +
                    "trim(c.1) AS COLUMN_NAME, " +
                    "c.2::Int16 AS KEY_SEQ, " +
                    "'PRIMARY' AS PK_NAME " +
                    "FROM system.tables " +
                    "ARRAY JOIN arrayZip(splitByChar(',', primary_key), arrayEnumerate(splitByChar(',', primary_key))) as c " +
                    "WHERE system.tables.primary_key <> '' " +
                    "AND system.tables.database ILIKE '" + (schema == null ? "%" : schema) + "' " +
                    "AND system.tables.name ILIKE '" + (table == null ? "%" : table) + "' " +
                    "ORDER BY COLUMN_NAME";
            return connection.createStatement().executeQuery(sql);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        // ClickHouse has no notion of foreign key. This method should return empty resultset
        try {
            String sql = "SELECT NULL::Nullable(String) AS PKTABLE_CAT, " +
                    "NULL::Nullable(String) AS PKTABLE_SCHEM, " +
                    "NULL::Nullable(String) AS PKTABLE_NAME, " +
                    "NULL::Nullable(String) AS PKCOLUMN_NAME, " +
                    "NULL::Nullable(String) AS FKTABLE_CAT, " +
                    "NULL::Nullable(String) AS FKTABLE_SCHEM, " +
                    "NULL::Nullable(String) AS FKTABLE_NAME, " +
                    "NULL::Nullable(String) AS FKCOLUMN_NAME, " +
                    "NULL::Nullable(Int16) AS KEY_SEQ, " +
                    "NULL::Nullable(Int16) AS UPDATE_RULE, " +
                    "NULL::Nullable(Int16) AS DELETE_RULE, " +
                    "NULL::Nullable(String) AS FK_NAME, " +
                    "NULL::Nullable(String) AS PK_NAME, " +
                    "NULL::Nullable(Int16) AS DEFERRABILITY" +
                    " LIMIT 0";
            return connection.createStatement().executeQuery(sql);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        // ClickHouse has no notion of foreign key. This method should return empty resultset
        try {
            return connection.createStatement().executeQuery("SELECT NULL::Nullable(String) AS PKTABLE_CAT, " +
                    "NULL::Nullable(String) AS PKTABLE_SCHEM, " +
                    "NULL::Nullable(String) AS PKTABLE_NAME, " +
                    "NULL::Nullable(String) AS PKCOLUMN_NAME, " +
                    "NULL::Nullable(String) AS FKTABLE_CAT, " +
                    "NULL::Nullable(String) AS FKTABLE_SCHEM, " +
                    "NULL::Nullable(String) AS FKTABLE_NAME, " +
                    "NULL::Nullable(String) AS FKCOLUMN_NAME, " +
                    "NULL::Nullable(Int16) AS KEY_SEQ, " +
                    "NULL::Nullable(Int16) AS UPDATE_RULE, " +
                    "NULL::Nullable(Int16) AS DELETE_RULE, " +
                    "NULL::Nullable(String) AS FK_NAME, " +
                    "NULL::Nullable(String) AS PK_NAME, " +
                    "NULL::Nullable(Int16) AS DEFERRABILITY" +
                    " LIMIT 0");
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        //Return an empty result set with the required columns
        try {
            String columns = "NULL ::Nullable(String) AS PKTABLE_CAT, " +
                    "NULL::Nullable(String) AS PKTABLE_SCHEM, " +
                    "NULL::Nullable(String) AS PKTABLE_NAME, " +
                    "NULL::Nullable(String) AS PKCOLUMN_NAME, " +
                    "NULL::Nullable(String) AS FKTABLE_CAT, " +
                    "NULL::Nullable(String) AS FKTABLE_SCHEM, " +
                    "NULL::Nullable(String) AS FKTABLE_NAME, " +
                    "NULL::Nullable(String) AS FKCOLUMN_NAME, " +
                    "NULL::Nullable(Int16) AS KEY_SEQ, " +
                    "NULL::Nullable(Int16) AS UPDATE_RULE, " +
                    "NULL::Nullable(Int16) AS DELETE_RULE, " +
                    "NULL::Nullable(String) AS FK_NAME, " +
                    "NULL::Nullable(String) AS PK_NAME, " +
                    "NULL::Nullable(Int16) AS DEFERRABILITY" +
                    " LIMIT 0";
            return connection.createStatement().executeQuery("SELECT " + columns);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    @SuppressWarnings({"squid:S2095"})
    public ResultSet getTypeInfo() throws SQLException {
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(DATA_TYPE_INFO_SQL)) {
            return DetachedResultSet.createFromResultSet(rs, connection.getDefaultCalendar(), GET_TYPE_INFO_MUTATORS);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    private static final Consumer<Map<String,Object>> NULLABILITY_VALUE_FUNCTION = (row) -> {
        String type = (String) row.get("TYPE_NAME");
        int nullability= java.sql.DatabaseMetaData.typeNoNulls;
        if (type.equals(ClickHouseDataType.Nullable.name()) || type.equals(ClickHouseDataType.Dynamic.name())) {
            nullability = java.sql.DatabaseMetaData.typeNullable;
        }
        row.put("NULLABLE", nullability);
    };

    private static final List<Consumer<Map<String, Object>>> GET_TYPE_INFO_MUTATORS = Arrays.asList(
            DATA_TYPE_VALUE_FUNCTION,
            NULLABILITY_VALUE_FUNCTION
    );

    private static final String DATA_TYPE_INFO_SQL = getDataTypeInfoSql();

    private static String getDataTypeInfoSql() {
        StringBuilder sql = new StringBuilder("SELECT " +
                "name AS TYPE_NAME, " +
                "0::Int32 AS DATA_TYPE, " + // placeholder for data type int value
                "attrs.c2::Nullable(Int32) AS PRECISION, " +
                "NULL::Nullable(String) AS LITERAL_PREFIX, " +
                "NULL::Nullable(String) AS LITERAL_SUFFIX, " +
                "NULL::Nullable(String) AS CREATE_PARAMS, " +
                "0::Int16 AS NULLABLE, " + // placeholder for int value
                "not(dt.case_insensitive)::Boolean AS CASE_SENSITIVE, " +
                java.sql.DatabaseMetaData.typeSearchable + "::Int16 AS SEARCHABLE, " +
                "not(attrs.c3)::Boolean AS UNSIGNED_ATTRIBUTE, " +
                "false AS FIXED_PREC_SCALE, " +
                "false AS AUTO_INCREMENT, " +
                "if(empty(alias_to), name, alias_to) AS LOCAL_TYPE_NAME, " +
                "attrs.c4::Nullable(Int16) AS MINIMUM_SCALE, " +
                "attrs.c5::Nullable(Int16) AS MAXIMUM_SCALE, " +
                "0::Nullable(Int32) AS SQL_DATA_TYPE, " +
                "0::Nullable(Int32) AS SQL_DATETIME_SUB, " +
                "0::Nullable(Int32) AS NUM_PREC_RADIX " +
                "FROM system.data_type_families dt " +
                " LEFT JOIN (SELECT * FROM VALUES ( ");
        for (ClickHouseDataType type : ClickHouseDataType.values()) {
            sql.append("(")
                    .append('\'').append(type.name()).append("',") // c1
                    .append(type.getMaxPrecision()).append(',') // c2
                    .append(type.isSigned()).append(',') // c3
                    .append(type.getMinScale()).append(',') // c4
                    .append(type.getMaxScale()) // c5
                    .append("),");
        }
        sql.setLength(sql.length() - 1);
        sql.append(") ");

        sql.append(") as attrs ON (dt.name = attrs.c1)")
                .append(" WHERE alias_to == ''");
        return sql.toString();
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        try {
            String sql = "SELECT " +
                "null::Nullable(String) AS TABLE_CAT, " +
                "null::Nullable(String) AS TABLE_SCHEM, " +
                "null::Nullable(String) AS TABLE_NAME, " +
                "null::Nullable(Boolean) AS NON_UNIQUE, " +
                "null::Nullable(String) AS INDEX_QUALIFIER, " +
                "null::Nullable(String) AS INDEX_NAME, " +
                "null::Nullable(Int16) AS TYPE, " +
                "null::Nullable(Int16) AS ORDINAL_POSITION, " +
                "null::Nullable(String) AS COLUMN_NAME, " +
                "null::Nullable(String) AS ASC_OR_DESC, " +
                "null::Nullable(Int64) AS CARDINALITY, " +
                "null::Nullable(Int64) AS PAGES, " +
                "null::Nullable(String) AS FILTER_CONDITION " +
                    " LIMIT 0";
            return connection.createStatement().executeQuery(sql);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
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
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
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
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        //Return an empty result set with the required columns
        try {
            return connection.createStatement().executeQuery("SELECT " +
                    "NULL::Nullable(String) AS TYPE_CAT, " +
                    "NULL::Nullable(String) AS TYPE_SCHEM, " +
                    "NULL::Nullable(String) AS TYPE_NAME, " +
                    "NULL::Nullable(String) AS CLASS_NAME, " +
                    "NULL::Nullable(Int32) AS DATA_TYPE, " +
                    "NULL::Nullable(String) AS REMARKS, " +
                    "NULL::Nullable(Int16) AS BASE_TYPE" +
                    " LIMIT 0");
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
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
        // TODO: it should be true
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        // TODO: should be explained
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        // TODO: update when implemented in ClickHouse
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        //Return an empty result set with the required columns
        try {
            return connection.createStatement().executeQuery(
                    "SELECT NULL::Nullable(String) AS TYPE_CAT, "
                    + "NULL::Nullable(String) AS TYPE_SCHEM, "
                    + "NULL::Nullable(String) AS TYPE_NAME, "
                    + "NULL::Nullable(String) AS SUPERTYPE_CAT, "
                    + "NULL::Nullable(String) AS SUPERTYPE_SCHEM, "
                    + "NULL::Nullable(String) AS SUPERTYPE_NAME" +
                            " LIMIT 0");
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        //Return an empty result set with the required columns
        try {
            return connection.createStatement().executeQuery(
                    "SELECT "
                    + "NULL::Nullable(String) AS TABLE_CAT, "
                    + "NULL::Nullable(String) AS TABLE_SCHEM, "
                    + "NULL::Nullable(String) AS TABLE_NAME, "
                    + "NULL::Nullable(String) AS SUPERTABLE_NAME" +
                        " LIMIT 0");
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        //Return an empty result set with the required columns
        try {
            return connection.createStatement().executeQuery(
                    "SELECT "
                    + "NULL::Nullable(String) AS TYPE_CAT, "
                    + "NULL::Nullable(String) AS TYPE_SCHEM, "
                    + "NULL::Nullable(String) AS TYPE_NAME, "
                    + "NULL::Nullable(String) AS ATTR_NAME, "
                    + "NULL::Nullable(Int32) AS DATA_TYPE, "
                    + "NULL::Nullable(String) AS ATTR_TYPE_NAME, "
                    + "NULL::Nullable(Int32) AS ATTR_SIZE, "
                    + "NULL::Nullable(Int32) AS DECIMAL_DIGITS, "
                    + "NULL::Nullable(Int32) AS NUM_PREC_RADIX, "
                    + "NULL::Nullable(Int32) AS NULLABLE, "
                    + "NULL::Nullable(String) AS REMARKS, "
                    + "NULL::Nullable(String) AS ATTR_DEF, "
                    + "NULL::Nullable(Int32) AS SQL_DATA_TYPE, "
                    + "NULL::Nullable(Int32) AS SQL_DATETIME_SUB, "
                    + "NULL::Nullable(Int32) AS CHAR_OCTET_LENGTH, "
                    + "NULL::Nullable(Int32) AS ORDINAL_POSITION, "
                    + "NULL::Nullable(String) AS IS_NULLABLE, "
                    + "NULL::Nullable(String) AS SCOPE_CATALOG, "
                    + "NULL::Nullable(String) AS SCOPE_SCHEMA, "
                    + "NULL::Nullable(String) AS SCOPE_TABLE, "
                    + "NULL::Nullable(Int16) AS SOURCE_DATA_TYPE" +
                        " LIMIT 0");
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        String version = connection.getServerVersion();
        try {
            return Integer.parseInt(version.split("\\.")[0]);
        } catch (NumberFormatException e) {
            throw new SQLException("Failed to parse major version from server version: " + version, ExceptionUtils.SQL_STATE_CLIENT_ERROR, e);
        }
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        String version = connection.getServerVersion();
        try {
            return Integer.parseInt(version.split("\\.")[1]);
        } catch (NumberFormatException e) {
            log.error("Failed to parse minor version from server version: " + version, e);
            throw new SQLException("Failed to parse minor version from server version: " + version, ExceptionUtils.SQL_STATE_CLIENT_ERROR, e);
        }
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return Driver.getDriverMajorVersion();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return Driver.getDriverMinorVersion();
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL; // SQL:2003 standard
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
        // TODO: handle useCatalogs == true and return schema catalog name
        try {
            return connection.createStatement().executeQuery("SELECT name AS TABLE_SCHEM, " + catalogPlaceholder + " AS TABLE_CATALOG FROM system.databases " +
                    "WHERE name LIKE '" + (schemaPattern == null ? "%" : schemaPattern) + "'");
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    private static final String CLIENT_INFO_PROPERTIES_SQL = getClientInfoPropertiesSql();

    private static String getClientInfoPropertiesSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT c1 as NAME, c2 as MAX_LEN, c3 as DEFAULT_VALUE, c4 as DESCRIPTION FROM VALUES (");
        Arrays.stream(ClientInfoProperties.values()).forEach(p -> {
            sql.append("('").append(p.getKey()).append("', ");
            sql.append(p.getMaxValue()).append(", ");
            sql.append("'").append(p.getDefaultValue()).append("', ");
            sql.append("'").append(p.getDescription()).append("'), ");
        });
        sql.setLength(sql.length() - 2);
        sql.append(")");
        return sql.toString();
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        try {
            return connection.createStatement().executeQuery(CLIENT_INFO_PROPERTIES_SQL);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        String sql = "SELECT " +
                "NULL::Nullable(String) AS FUNCTION_CAT, " +
                "NULL::Nullable(String) AS FUNCTION_SCHEM, " +
                "name::Nullable(String) AS FUNCTION_NAME, " +
                "concat(description, '(', origin, ')') AS REMARKS, " +
                java.sql.DatabaseMetaData.functionResultUnknown + "::Int16 AS FUNCTION_TYPE, " +
                "name AS SPECIFIC_NAME " +
                "FROM system.functions " +
                "WHERE name LIKE '" + (functionNamePattern == null ? "%" : functionNamePattern) + "'";
        try {
            return connection.createStatement().executeQuery(sql);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        String sql = "SELECT " +
                "'' AS FUNCTION_CAT, " +
                "'' AS FUNCTION_SCHEM, " +
                "'' AS FUNCTION_NAME, " +
                "'' AS COLUMN_NAME, " +
                "0::Int16 AS COLUMN_TYPE, " +
                "0::Int32 AS DATA_TYPE, " +
                "'' AS TYPE_NAME, " +
                "0::Int32 AS PRECISION, " +
                "0::Int32 AS LENGTH, " +
                "0::Int16 AS SCALE, " +
                "0::Int16 AS RADIX, " +
                "0::Int16 AS NULLABLE, " +
                "'' AS REMARKS, " +
                "0::Int32 AS CHAR_OCTET_LENGTH, " +
                "0::Int32 AS ORDINAL_POSITION, " +
                "'' AS IS_NULLABLE, " +
                "'' AS SPECIFIC_NAME " +
                "LIMIT 0";

        try {
            return connection.createStatement().executeQuery(sql);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        String sql = "SELECT " +
                "'' AS TABLE_CAT, " +
                "'' AS TABLE_SCHEM, " +
                "'' AS TABLE_NAME, " +
                "'' AS COLUMN_NAME, " +
                "0::Int32 AS DATA_TYPE, " +
                "0::Int32 AS COLUMN_SIZE, " +
                "0::Int32 AS DECIMAL_DIGITS, " +
                "0::Int32 AS NUM_PREC_RADIX, " +
                "'' AS COLUMN_USAGE, " +
                "'' AS REMARKS, " +
                "0::Int32 AS CHAR_OCTET_LENGTH, " +
                "'' AS IS_NULLABLE " +
                " LIMIT 0";

        try {
            return connection.createStatement().executeQuery(sql);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public long getMaxLogicalLobSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsRefCursors() throws SQLException {
        return false;
    }
}
