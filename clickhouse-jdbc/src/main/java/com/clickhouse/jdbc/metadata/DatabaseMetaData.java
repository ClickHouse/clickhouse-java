package com.clickhouse.jdbc.metadata;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;


import com.clickhouse.jdbc.ConnectionImpl;
import com.clickhouse.jdbc.Driver;
import com.clickhouse.jdbc.JdbcWrapper;
import com.clickhouse.jdbc.ResultSetImpl;
import com.clickhouse.jdbc.internal.JdbcUtils;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

public class DatabaseMetaData implements java.sql.DatabaseMetaData, JdbcWrapper {
    private static final Logger log = LoggerFactory.getLogger(DatabaseMetaData.class);
    ConnectionImpl connection;

    public DatabaseMetaData(ConnectionImpl connection) {
        this.connection = connection;
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
        return connection.getURL();
    }

    @Override
    public String getUserName() throws SQLException {
        return connection.getUser();
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
        return "ClickHouse";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "ClickHouse JDBC Driver";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return Driver.driverVersion;
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

    @Override
    public String getSchemaTerm() throws SQLException {
        return "database";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "database";
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
        return true;
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
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return true;
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
        return connection.getTransactionIsolation();
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level == connection.getTransactionIsolation();
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
        //TODO: Drill into this
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        String sql = "SELECT database AS TABLE_CAT, database AS TABLE_SCHEM, name AS TABLE_NAME, engine AS TABLE_TYPE," +
                " '' AS REMARKS, '' AS TYPE_CAT, '' AS TYPE_SCHEM, '' AS TYPE_NAME, '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION" +
                " FROM system.tables" +
                " WHERE database LIKE '" + (schemaPattern == null ? "%" : schemaPattern) + "'" +
                " AND database LIKE '" + (catalog == null ? "%" : catalog) + "'" +
                " AND name LIKE '" + (tableNamePattern == null ? "%" : tableNamePattern) + "'";
        if (types != null && types.length > 0) {
            sql += "AND engine IN (";
            for (String type : types) {
                sql += "'" + type + "',";
            }
            sql = sql.substring(0, sql.length() - 1) + ") ";
        }
        return connection.createStatement().executeQuery(sql);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return connection.createStatement().executeQuery("SELECT name AS TABLE_SCHEM, null AS TABLE_CATALOG FROM system.databases ORDER BY name");
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return connection.createStatement().executeQuery("SELECT name AS TABLE_CAT FROM system.databases ORDER BY name");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        //https://clickhouse.com/docs/en/engines/table-engines/
        return connection.createStatement().executeQuery("SELECT c1 AS TABLE_TYPE " +
                "FROM VALUES ('MergeTree','ReplacingMergeTree','SummingMergeTree','AggregatingMergeTree','CollapsingMergeTree','VersionedCollapsingMergeTree','GraphiteMergeTree'," +
                "'TinyLog','StripeLog','Log'," +
                "'ODBC','JDBC','MySQL','MongoDB','Redis','HDFS','S3','Kafka','EmbeddedRocksDB','RabbitMQ','PostgreSQL','S3Queue','TimeSeries'," +
                "'Distributed','Dictionary','Merge','File','Null','Set','Join','URL','View','Memory','Buffer','KeeperMap')");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        //TODO: Best way to convert type to JDBC data type
        String sql = "SELECT system.columns.database AS TABLE_CAT, system.columns.database AS TABLE_SCHEM, system.columns.table AS TABLE_NAME, system.columns.name AS COLUMN_NAME, " +
                JdbcUtils.generateSqlTypeEnum("system.columns.type") + " AS DATA_TYPE, system.columns.type AS TYPE_NAME, toInt32(system.columns.numeric_precision) AS COLUMN_SIZE, toInt32(0) AS BUFFER_LENGTH, toInt32(system.columns.numeric_precision) AS DECIMAL_DIGITS," +
                " toInt32(system.columns.numeric_precision_radix) AS NUM_PREC_RADIX, toInt32(2) AS NULLABLE, system.columns.comment AS REMARKS, system.columns.default_expression AS COLUMN_DEF, toInt32(0) AS SQL_DATA_TYPE," +
                " 0 AS SQL_DATETIME_SUB, 0 AS CHAR_OCTET_LENGTH, toInt32(system.columns.position) AS ORDINAL_POSITION, '' AS IS_NULLABLE," +
                " NULL AS SCOPE_CATALOG, NULL AS SCOPE_SCHEMA, NULL AS SCOPE_TABLE, NULL AS SOURCE_DATA_TYPE" +
                " FROM system.columns" +
                " WHERE database LIKE '" + (schemaPattern == null ? "%" : schemaPattern) + "'" +
                " AND database LIKE '" + (catalog == null ? "%" : catalog) + "'" +
                " AND table LIKE '" + (tableNamePattern == null ? "%" : tableNamePattern) + "'" +
                " AND name LIKE '" + (columnNamePattern == null ? "%" : columnNamePattern) + "'";
        return connection.createStatement().executeQuery(sql);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        //Return an empty result set with the required columns
        log.warn("getColumnPrivileges is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NULL AS TABLE_NAME, NULL AS COLUMN_NAME, NULL AS GRANTOR, NULL AS GRANTEE, NULL AS PRIVILEGE, NULL AS IS_GRANTABLE");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        //Return an empty result set with the required columns
        log.warn("getTablePrivileges is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NULL AS TABLE_NAME, NULL AS GRANTOR, NULL AS GRANTEE, NULL AS PRIVILEGE, NULL AS IS_GRANTABLE");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        //Return an empty result set with the required columns
        log.warn("getBestRowIdentifier is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS SCOPE, NULL AS COLUMN_NAME, NULL AS DATA_TYPE, NULL AS TYPE_NAME, NULL AS COLUMN_SIZE, NULL AS BUFFER_LENGTH, NULL AS DECIMAL_DIGITS, NULL AS PSEUDO_COLUMN");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        //Return an empty result set with the required columns
        log.warn("getVersionColumns is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS SCOPE, NULL AS COLUMN_NAME, NULL AS DATA_TYPE, NULL AS TYPE_NAME, NULL AS COLUMN_SIZE, NULL AS BUFFER_LENGTH, NULL AS DECIMAL_DIGITS, NULL AS PSEUDO_COLUMN");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        //Return an empty result set with the required columns
        log.warn("getPrimaryKeys is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NULL AS TABLE_NAME, NULL AS COLUMN_NAME, NULL AS KEY_SEQ, NULL AS PK_NAME");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        //Return an empty result set with the required columns
        log.warn("getImportedKeys is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM, NULL AS PKTABLE_NAME, NULL AS PKCOLUMN_NAME, NULL AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, NULL AS FKTABLE_NAME, NULL AS FKCOLUMN_NAME, NULL AS KEY_SEQ, NULL AS UPDATE_RULE, NULL AS DELETE_RULE, NULL AS FK_NAME, NULL AS PK_NAME, NULL AS DEFERRABILITY");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        //Return an empty result set with the required columns
        log.warn("getExportedKeys is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM, NULL AS PKTABLE_NAME, NULL AS PKCOLUMN_NAME, NULL AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, NULL AS FKTABLE_NAME, NULL AS FKCOLUMN_NAME, NULL AS KEY_SEQ, NULL AS UPDATE_RULE, NULL AS DELETE_RULE, NULL AS FK_NAME, NULL AS PK_NAME, NULL AS DEFERRABILITY");
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        //Return an empty result set with the required columns
        log.warn("getCrossReference is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM, NULL AS PKTABLE_NAME, NULL AS PKCOLUMN_NAME, NULL AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, NULL AS FKTABLE_NAME, NULL AS FKCOLUMN_NAME, NULL AS KEY_SEQ, NULL AS UPDATE_RULE, NULL AS DELETE_RULE, NULL AS FK_NAME, NULL AS PK_NAME");
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return null;
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
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        //Return an empty result set with the required columns
        log.warn("getUDTs is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS CLASS_NAME, NULL AS DATA_TYPE, NULL AS REMARKS, NULL AS BASE_TYPE");
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
        //Return an empty result set with the required columns
        log.warn("getSuperTypes is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SUPERTYPE_CAT, NULL AS SUPERTYPE_SCHEM, NULL AS SUPERTYPE_NAME");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        //Return an empty result set with the required columns
        log.warn("getSuperTables is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NULL AS TABLE_NAME, NULL AS SUPERTABLE_NAME");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        //Return an empty result set with the required columns
        log.warn("getAttributes is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS ATTR_NAME, NULL AS DATA_TYPE, NULL AS ATTR_TYPE_NAME, NULL AS ATTR_SIZE, NULL AS DECIMAL_DIGITS, NULL AS NUM_PREC_RADIX, NULL AS NULLABLE, NULL AS REMARKS, NULL AS ATTR_DEF, NULL AS SQL_DATA_TYPE, NULL AS SQL_DATETIME_SUB, NULL AS CHAR_OCTET_LENGTH, NULL AS ORDINAL_POSITION, NULL AS IS_NULLABLE, NULL AS SCOPE_CATALOG, NULL AS SCOPE_SCHEMA, NULL AS SCOPE_TABLE, NULL AS SOURCE_DATA_TYPE");
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
        return connection.getMajorVersion();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return connection.getMinorVersion();
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
        return connection.createStatement().executeQuery("SELECT name AS TABLE_SCHEM, null AS TABLE_CATALOG FROM system.databases WHERE name LIKE '" + (schemaPattern == null ? "%" : schemaPattern) + "'");
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
        //Return an empty result set with the required columns
        log.warn("getClientInfoProperties is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS NAME, NULL AS MAX_LEN, NULL AS DEFAULT_VALUE, NULL AS DESCRIPTION");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        //Return an empty result set with the required columns
        log.warn("getFunctions is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS FUNCTION_CAT, NULL AS FUNCTION_SCHEM, NULL AS FUNCTION_NAME, NULL AS REMARKS, NULL AS FUNCTION_TYPE, NULL AS SPECIFIC_NAME");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        //Return an empty result set with the required columns
        log.warn("getFunctionColumns is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS FUNCTION_CAT, NULL AS FUNCTION_SCHEM, NULL AS FUNCTION_NAME, NULL AS COLUMN_NAME, NULL AS COLUMN_TYPE, NULL AS DATA_TYPE, NULL AS TYPE_NAME, NULL AS PRECISION, NULL AS LENGTH, NULL AS SCALE, NULL AS RADIX, NULL AS NULLABLE, NULL AS REMARKS, NULL AS CHAR_OCTET_LENGTH, NULL AS ORDINAL_POSITION, NULL AS IS_NULLABLE, NULL AS SPECIFIC_NAME");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        //Return an empty result set with the required columns
        log.warn("getPseudoColumns is not supported and may return invalid results");
        return connection.createStatement().executeQuery("SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NULL AS TABLE_NAME, NULL AS COLUMN_NAME, NULL AS DATA_TYPE, NULL AS COLUMN_SIZE, NULL AS DECIMAL_DIGITS, NULL AS NUM_PREC_RADIX, NULL AS COLUMN_USAGE, NULL AS REMARKS, NULL AS CHAR_OCTET_LENGTH, NULL AS IS_NULLABLE");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public long getMaxLogicalLobSize() throws SQLException {
        return java.sql.DatabaseMetaData.super.getMaxLogicalLobSize();
    }

    @Override
    public boolean supportsRefCursors() throws SQLException {
        return java.sql.DatabaseMetaData.super.supportsRefCursors();
    }

    @Override
    public boolean supportsSharding() throws SQLException {
        return java.sql.DatabaseMetaData.super.supportsSharding();
    }
}
