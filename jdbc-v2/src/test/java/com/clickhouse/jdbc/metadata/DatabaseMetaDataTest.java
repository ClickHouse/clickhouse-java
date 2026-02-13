package com.clickhouse.jdbc.metadata;

import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.jdbc.ClientInfoProperties;
import com.clickhouse.jdbc.DriverProperties;
import com.clickhouse.jdbc.JdbcIntegrationTest;
import com.clickhouse.jdbc.internal.JdbcUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.sql.RowIdLifetime.ROWID_UNSUPPORTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


@Test(groups = { "integration" })
public class DatabaseMetaDataTest extends JdbcIntegrationTest {
    @Test(groups = { "integration" })
    public void testGetColumns() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            final String tableName = "get_columns_metadata_test";
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("" +
                        "CREATE TABLE " + tableName + " (id Int32, name String NOT NULL, v1 Nullable(Int8), v2 Array(Int8)) " +
                        "ENGINE MergeTree ORDER BY tuple()");
            }

            DatabaseMetaData dbmd = conn.getMetaData();


            try (ResultSet rs = dbmd.getColumns(null, getDatabase(), tableName.substring(0, tableName.length() - 3) + "%", null)) {

                List<String> expectedColumnNames = Arrays.asList(
                        "TABLE_CAT",
                        "TABLE_SCHEM",
                        "TABLE_NAME",
                        "COLUMN_NAME",
                        "DATA_TYPE",
                        "TYPE_NAME",
                        "COLUMN_SIZE",
                        "BUFFER_LENGTH",
                        "DECIMAL_DIGITS",
                        "NUM_PREC_RADIX",
                        "NULLABLE",
                        "REMARKS",
                        "COLUMN_DEF",
                        "SQL_DATA_TYPE",
                        "SQL_DATETIME_SUB",
                        "CHAR_OCTET_LENGTH",
                        "ORDINAL_POSITION",
                        "IS_NULLABLE",
                        "SCOPE_CATALOG",
                        "SCOPE_SCHEMA",
                        "SCOPE_TABLE",
                        "SOURCE_DATA_TYPE",
                        "IS_AUTOINCREMENT",
                        "IS_GENERATEDCOLUMN"
                );

                List<Integer> expectedColumnTypes = Arrays.asList(
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.INTEGER,
                        Types.VARCHAR,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.SMALLINT,
                        Types.VARCHAR,
                        Types.VARCHAR
                );

                assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);

                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_SCHEM"), getDatabase());
                assertEquals(rs.getString("TABLE_NAME"), tableName);
                assertEquals(rs.getString("COLUMN_NAME"), "id");
                assertEquals(rs.getInt("DATA_TYPE"), Types.INTEGER);
                assertEquals(rs.getObject("DATA_TYPE"), Types.INTEGER);
                assertEquals(rs.getString("TYPE_NAME"), "Int32");
                assertFalse(rs.getBoolean("NULLABLE"));

                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_SCHEM"), getDatabase());
                assertEquals(rs.getString("TABLE_NAME"), tableName);
                assertEquals(rs.getString("COLUMN_NAME"), "name");
                assertEquals(rs.getInt("DATA_TYPE"), Types.VARCHAR);
                assertEquals(rs.getObject("DATA_TYPE"), Types.VARCHAR);
                assertEquals(rs.getString("TYPE_NAME"), "String");
                assertFalse(rs.getBoolean("NULLABLE"));

                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_SCHEM"), getDatabase());
                assertEquals(rs.getString("TABLE_NAME"), tableName);
                assertEquals(rs.getString("COLUMN_NAME"), "v1");
                assertEquals(rs.getInt("DATA_TYPE"), Types.TINYINT);
                assertEquals(rs.getObject("DATA_TYPE"), Types.TINYINT);
                assertEquals(rs.getString("TYPE_NAME"), "Nullable(Int8)");
                assertTrue(rs.getBoolean("NULLABLE"));

                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_SCHEM"), getDatabase());
                assertEquals(rs.getString("TABLE_NAME"), tableName);
                assertEquals(rs.getString("COLUMN_NAME"), "v2");
                assertEquals(rs.getInt("DATA_TYPE"), Types.ARRAY);
                assertEquals(rs.getObject("DATA_TYPE"), Types.ARRAY);
                assertEquals(rs.getString("TYPE_NAME"), "Array(Int8)");
                assertFalse(rs.getBoolean("NULLABLE"));
            }
        }
    }

    @Test(groups = {"integration"})
    public void testSupportFlags() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();


            assertEquals(dbmd.getMaxConnections(), 150);
            assertEquals(dbmd.isReadOnly(), false);
            assertEquals(dbmd.getDriverName(), "ClickHouse JDBC Driver");
            assertEquals(dbmd.getSchemaTerm(), "schema");
            assertEquals(dbmd.getDatabaseProductName(), "ClickHouse");
            assertEquals(dbmd.supportsMixedCaseIdentifiers(), true);
            assertEquals(dbmd.supportsMixedCaseQuotedIdentifiers(), false);
            assertEquals(dbmd.getIdentifierQuoteString(), "`");
            assertEquals(dbmd.getSQLKeywords(), "APPLY,ASOF,ATTACH,CLUSTER,DATABASE,DATABASES,DETACH,DICTIONARY,DICTIONARIES,ILIKE,INF,LIMIT,LIVE,KILL,MATERIALIZED,NAN,OFFSET,OPTIMIZE,OUTFILE,POLICY,PREWHERE,PROFILE,QUARTER,QUOTA,RENAME,REPLACE,SAMPLE,SETTINGS,SHOW,TABLES,TIES,TOP,TOTALS,TRUNCATE,USE,WATCH,WEEK");
            assertEquals(dbmd.getNumericFunctions(), "abs,acos,acosh,asin,asinh,atan,atan2,atanh,cbrt,cos,cosh,divide,e,erf,erfc,exp,exp10,exp2,gcd,hypot,intDiv,intDivOrZero,intExp10,intExp2,lcm,lgamma,ln,log,log10,log1p,log2,minus,modulo,moduloOrZero,multiply,negate,pi,plus,pow,power,sign,sin,sinh,sqrt,tan,tgamma");
            assertEquals(dbmd.getStringFunctions(), "appendTrailingCharIfAbsent,base64Decode,base64Encode,char_length,CHAR_LENGTH,character_length,CHARACTER_LENGTH,concat,concatAssumeInjective,convertCharset,countMatches,countSubstrings,countSubstringsCaseInsensitive,countSubstringsCaseInsensitiveUTF8,CRC32,CRC32IEEE,CRC64,decodeXMLComponent,empty,encodeXMLComponent,endsWith,extract,extractAll,extractAllGroupsHorizontal,extractAllGroupsVertical,extractTextFromHTML ,format,ilike,isValidUTF8,lcase,leftPad,leftPadUTF8,length,lengthUTF8,like,locate,lower,lowerUTF8,match,mid,multiFuzzyMatchAllIndices,multiFuzzyMatchAny,multiFuzzyMatchAnyIndex,multiMatchAllIndices,multiMatchAny,multiMatchAnyIndex,multiSearchAllPositions,multiSearchAllPositionsUTF8,multiSearchAny,multiSearchFirstIndex,multiSearchFirstPosition,ngramDistance,ngramSearch,normalizedQueryHash,normalizeQuery,notEmpty,notLike,position,positionCaseInsensitive,positionCaseInsensitiveUTF8,positionUTF8,regexpQuoteMeta,repeat,replace,replaceAll,replaceOne,replaceRegexpAll,replaceRegexpOne,reverse,reverseUTF8,rightPad,rightPadUTF8,startsWith,substr,substring,substringUTF8,tokens,toValidUTF8,trim,trimBoth,trimLeft,trimRight,tryBase64Decode,ucase,upper,upperUTF8");
            assertEquals(dbmd.getSystemFunctions(), "bar,basename,blockNumber,blockSerializedSize,blockSize,buildId,byteSize,countDigits,currentDatabase,currentProfiles,currentRoles,currentUser,defaultProfiles,defaultRoles,defaultValueOfArgumentType,defaultValueOfTypeName,dumpColumnStructure,enabledProfiles,enabledRoles,errorCodeToName,filesystemAvailable,filesystemCapacity,filesystemFree,finalizeAggregation,formatReadableQuantity,formatReadableSize,formatReadableTimeDelta,FQDN,getMacro,getServerPort,getSetting,getSizeOfEnumType,greatest,hasColumnInTable,hostName,identity,ifNotFinite,ignore,indexHint,initializeAggregation,initialQueryID,isConstant,isDecimalOverflow,isFinite,isInfinite,isNaN,joinGet,least,MACNumToString,MACStringToNum,MACStringToOUI,materialize,modelEvaluate,neighbor,queryID,randomFixedString,randomPrintableASCII,randomString,randomStringUTF8,replicate,rowNumberInAllBlocks,rowNumberInBlock,runningAccumulate,runningConcurrency,runningDifference,runningDifferenceStartingWithFirstValue,shardCount ,shardNum,sleep,sleepEachRow,tcpPort,throwIf,toColumnTypeName,toTypeName,transform,uptime,version,visibleWidth");
            assertEquals(dbmd.getTimeDateFunctions(), "addDays,addHours,addMinutes,addMonths,addQuarters,addSeconds,addWeeks,addYears,date_add,date_diff,date_sub,date_trunc,dateName,formatDateTime,FROM_UNIXTIME,fromModifiedJulianDay,fromModifiedJulianDayOrNull,now,subtractDays,subtractHours,subtractMinutes,subtractMonths,subtractQuarters,subtractSeconds,subtractWeeks,subtractYears,timeSlot,timeSlots,timestamp_add,timestamp_sub,timeZone,timeZoneOf,timeZoneOffset,today,toDayOfMonth,toDayOfWeek,toDayOfYear,toHour,toISOWeek,toISOYear,toMinute,toModifiedJulianDay,toModifiedJulianDayOrNull,toMonday,toMonth,toQuarter,toRelativeDayNum,toRelativeHourNum,toRelativeMinuteNum,toRelativeMonthNum,toRelativeQuarterNum,toRelativeSecondNum,toRelativeWeekNum,toRelativeYearNum,toSecond,toStartOfDay,toStartOfFifteenMinutes,toStartOfFiveMinute,toStartOfHour,toStartOfInterval,toStartOfISOYear,toStartOfMinute,toStartOfMonth,toStartOfQuarter,toStartOfSecond,toStartOfTenMinutes,toStartOfWeek,toStartOfYear,toTime,toTimeZone,toUnixTimestamp,toWeek,toYear,toYearWeek,toYYYYMM,toYYYYMMDD,toYYYYMMDDhhmmss,yesterday");
            assertEquals(dbmd.getSearchStringEscape(), "\\");
                    assertEquals(dbmd.getExtraNameCharacters(), "");
            assertEquals(dbmd.supportsAlterTableWithAddColumn(), true);
            assertEquals(dbmd.supportsAlterTableWithDropColumn(), true);
            assertEquals(dbmd.supportsColumnAliasing(), true);
            assertEquals(dbmd.supportsConvert(), false);
            assertEquals(dbmd.supportsTableCorrelationNames(), true);
            assertEquals(dbmd.supportsDifferentTableCorrelationNames(), false);
            assertEquals(dbmd.supportsExpressionsInOrderBy(), true);
            assertEquals(dbmd.supportsOrderByUnrelated(), true);
            assertEquals(dbmd.supportsGroupBy(), true);
            assertEquals(dbmd.supportsGroupByUnrelated(), true);
            assertEquals(dbmd.supportsGroupByBeyondSelect(), true);
            assertEquals(dbmd.supportsLikeEscapeClause(), true);
            assertEquals(dbmd.supportsMultipleResultSets(), false);
            assertEquals(dbmd.supportsMultipleTransactions(), false);
            assertEquals(dbmd.supportsNonNullableColumns(), true);
            assertEquals(dbmd.supportsMinimumSQLGrammar(), true);
            assertEquals(dbmd.supportsCoreSQLGrammar(), true);
            assertEquals(dbmd.supportsExtendedSQLGrammar(), true);
            assertEquals(dbmd.supportsANSI92EntryLevelSQL(), true);
            assertEquals(dbmd.supportsANSI92IntermediateSQL(), false);
            assertEquals(dbmd.supportsANSI92FullSQL(), false);
            assertEquals(dbmd.supportsIntegrityEnhancementFacility(), false);
            assertEquals(dbmd.supportsOuterJoins(), true);
            assertEquals(dbmd.supportsFullOuterJoins(), true);
            assertEquals(dbmd.supportsLimitedOuterJoins(), true);
            assertEquals(dbmd.getProcedureTerm(), "function");
            assertEquals(dbmd.getCatalogTerm(), "cluster");
            assertEquals(dbmd.isCatalogAtStart(), true);
            assertEquals(dbmd.getCatalogSeparator(), ".");
            assertEquals(dbmd.supportsSchemasInDataManipulation(), true);
            assertEquals(dbmd.supportsSchemasInProcedureCalls(), false);
            assertEquals(dbmd.supportsSchemasInTableDefinitions(), true);
            assertEquals(dbmd.supportsSchemasInIndexDefinitions(), true);
            assertEquals(dbmd.supportsSchemasInPrivilegeDefinitions(), true);
            assertEquals(dbmd.supportsCatalogsInDataManipulation(), false);
            assertEquals(dbmd.supportsCatalogsInProcedureCalls(), false);
            assertEquals(dbmd.supportsCatalogsInTableDefinitions(), false);
            assertEquals(dbmd.supportsCatalogsInIndexDefinitions(), false);
            assertEquals(dbmd.supportsCatalogsInPrivilegeDefinitions(), false);
            assertEquals(dbmd.supportsPositionedDelete(), false);
            assertEquals(dbmd.supportsPositionedUpdate(), false);
            assertEquals(dbmd.supportsSelectForUpdate(), false);
            assertEquals(dbmd.supportsStoredProcedures(), false);
            assertEquals(dbmd.supportsSubqueriesInComparisons(), true);
            assertEquals(dbmd.supportsSubqueriesInExists(), false);
            assertEquals(dbmd.supportsSubqueriesInIns(), true);
            assertEquals(dbmd.supportsSubqueriesInQuantifieds(), true);
            assertEquals(dbmd.supportsCorrelatedSubqueries(), true);
            assertEquals(dbmd.supportsUnion(), true);
            assertEquals(dbmd.supportsUnionAll(), true);
            assertEquals(dbmd.supportsOpenCursorsAcrossCommit(), false);
            assertEquals(dbmd.supportsOpenCursorsAcrossRollback(), false);
            assertEquals(dbmd.supportsOpenStatementsAcrossCommit(), false);
            assertEquals(dbmd.supportsOpenStatementsAcrossRollback(), false);
            assertEquals(dbmd.getMaxBinaryLiteralLength(), Integer.MAX_VALUE);
            assertEquals(dbmd.getMaxCharLiteralLength(), Integer.MAX_VALUE);
            assertEquals(dbmd.getMaxColumnNameLength(), Short.MAX_VALUE);
            assertEquals(dbmd.getMaxColumnsInGroupBy(), 0);
            assertEquals(dbmd.getMaxColumnsInIndex(), 0);
            assertEquals(dbmd.getMaxColumnsInOrderBy(), 0);
            assertEquals(dbmd.getMaxColumnsInSelect(), 0);
            assertEquals(dbmd.getMaxColumnsInTable(), 1000);
            assertEquals(dbmd.getMaxCursorNameLength(), 0);
            assertEquals(dbmd.getMaxIndexLength(), 0);
            assertEquals(dbmd.getMaxSchemaNameLength(), Integer.MAX_VALUE);
            assertEquals(dbmd.getMaxProcedureNameLength(), 0);
            assertEquals(dbmd.getMaxCatalogNameLength(), 0);
            assertEquals(dbmd.getMaxRowSize(), 0);
            assertEquals(dbmd.getMaxStatementLength(), 0);
            assertEquals(dbmd.getMaxStatements(), 150);
            assertEquals(dbmd.getMaxTableNameLength(), Integer.MAX_VALUE);
            assertEquals(dbmd.getMaxTablesInSelect(), 0);
            assertEquals(dbmd.getMaxUserNameLength(), 0);
            assertEquals(dbmd.getDefaultTransactionIsolation(), 0);
            assertEquals(dbmd.supportsTransactions(), false);
            assertEquals(dbmd.supportsDataDefinitionAndDataManipulationTransactions(), false);
            assertEquals(dbmd.supportsDataManipulationTransactionsOnly(), false);
            assertEquals(dbmd.supportsBatchUpdates(), true);
            assertEquals(dbmd.supportsSavepoints(), false);
            assertEquals(dbmd.supportsNamedParameters(), false);
            assertEquals(dbmd.supportsMultipleOpenResults(), false);
            assertEquals(dbmd.supportsGetGeneratedKeys(), false);
            assertEquals(dbmd.getResultSetHoldability(), 1);
            assertEquals(dbmd.getJDBCMajorVersion(), 4); // Latest major version (since java 6).
            assertEquals(dbmd.getJDBCMinorVersion(), 2); // Most supported minor version (since java 8).
            assertEquals(dbmd.getSQLStateType(), 2);
            assertEquals(dbmd.supportsStatementPooling(), false);
            assertEquals(dbmd.getRowIdLifetime(), ROWID_UNSUPPORTED);
            assertEquals(dbmd.supportsStoredFunctionsUsingCallSyntax(), false);
            assertEquals(dbmd.getMaxLogicalLobSize(), 0);
            assertEquals(dbmd.supportsRefCursors(), false);
            assertEquals(dbmd.supportsSharding(), false);
            assertEquals(dbmd.allProceduresAreCallable(), true);
            assertEquals(dbmd.allTablesAreSelectable(), true);
            assertEquals(dbmd.nullsAreSortedHigh(), false);
            assertEquals(dbmd.nullsAreSortedLow(), true);
            assertEquals(dbmd.nullsAreSortedAtStart(), false);
            assertEquals(dbmd.nullsAreSortedAtEnd(), true);
            assertEquals(dbmd.usesLocalFiles(), false);
            assertEquals(dbmd.usesLocalFilePerTable(), false);
            assertEquals(dbmd.storesUpperCaseIdentifiers(), false);
            assertEquals(dbmd.storesLowerCaseIdentifiers(), true);
            assertEquals(dbmd.storesMixedCaseIdentifiers(), false);
            assertEquals(dbmd.storesUpperCaseQuotedIdentifiers(), false);
            assertEquals(dbmd.storesMixedCaseQuotedIdentifiers(), false);
            assertEquals(dbmd.nullPlusNonNullIsNull(), true);
            assertEquals(dbmd.supportsConvert(), false);

            assertEquals(dbmd.getDefaultTransactionIsolation(), Connection.TRANSACTION_NONE);
            assertEquals(dbmd.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE), true);
            for (int type : new int[] {Connection.TRANSACTION_SERIALIZABLE, Connection.TRANSACTION_READ_COMMITTED, Connection.TRANSACTION_READ_UNCOMMITTED, Connection.TRANSACTION_REPEATABLE_READ} ) {
                assertFalse(dbmd.supportsTransactionIsolationLevel(type));
            }
            assertEquals(dbmd.dataDefinitionCausesTransactionCommit(), false);
            assertEquals(dbmd.dataDefinitionIgnoredInTransactions(), false);
            assertEquals(dbmd.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY), true);

            for (int type : new int[] {ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE} ) {
                assertFalse(dbmd.supportsResultSetType(type));
                assertFalse(dbmd.ownUpdatesAreVisible(type));
                assertFalse(dbmd.ownDeletesAreVisible(type));
                assertFalse(dbmd.ownInsertsAreVisible(type));
                assertFalse(dbmd.othersUpdatesAreVisible(type));
                assertFalse(dbmd.othersDeletesAreVisible(type));
                assertFalse(dbmd.othersInsertsAreVisible(type));
                assertFalse(dbmd.updatesAreDetected(type));
                assertFalse(dbmd.deletesAreDetected(type));
                assertFalse(dbmd.insertsAreDetected(type));

                for (int concurType : new int[] {ResultSet.CONCUR_UPDATABLE, ResultSet.CONCUR_READ_ONLY}) {
                    assertFalse(dbmd.supportsResultSetConcurrency(type, concurType));
                }
            }

            assertFalse(dbmd.generatedKeyAlwaysReturned());
        }
    }

    @Test(groups = { "integration" })
    public void testGetColumnsWithTable() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            final String tableName = "test_get_columns_1";
            conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);

            StringBuilder createTableStmt = new StringBuilder("CREATE TABLE " + tableName + " (");
            List<String> columnNames = Arrays.asList("id", "huge_integer", "name", "float1", "fixed_string1", "decimal_1", "nullable_column", "date", "datetime");
            List<String> columnTypes = Arrays.asList("Int64", "UInt128", "String", "Float32", "FixedString(10)", "Decimal(10, 2)", "Nullable(Decimal(5, 4))", "Date", "DateTime");
            List<Integer> columnSizes = Arrays.asList(8, 16, 0, 4, 10, 10, 5, 2, 0);
            List<Integer> columnJDBCDataTypes = Arrays.asList(Types.BIGINT, Types.OTHER, Types.VARCHAR, Types.FLOAT, Types.VARCHAR, Types.DECIMAL, Types.DECIMAL, Types.DATE, Types.TIMESTAMP);
            List<String> columnTypeNames = Arrays.asList("Int64", "UInt128", "String", "Float32", "FixedString(10)", "Decimal(10, 2)", "Nullable(Decimal(5, 4))", "Date", "DateTime");
            List<Boolean> columnNullable = Arrays.asList(false, false, false, false, false, false, true, false, false);
            List<Integer> columnDecimalDigits = Arrays.asList(null, null, null, null, null, 2, 4, null, null);
            List<Integer> columnRadix = Arrays.asList(2, 2, null, null, null, 10, 10, null, null);

            for (int i = 0; i < columnNames.size(); i++) {
                createTableStmt.append(columnNames.get(i)).append(" ").append(columnTypes.get(i)).append(',');
            }
            createTableStmt.setLength(createTableStmt.length() - 1);
            createTableStmt.append(") ENGINE = MergeTree ORDER BY tuple()");
            conn.createStatement().execute(createTableStmt.toString());

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getColumns(null, ClickHouseServerForTest.getDatabase(), tableName, null);

            int count = 0;
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                int colIndex = columnNames.indexOf(columnName);
                System.out.println("Column name: " + columnName + " colIndex: " + colIndex);
                assertTrue(columnNames.contains(columnName));
                assertEquals(rs.getString("TABLE_CAT"), "");
                assertEquals(rs.getString("TABLE_SCHEM"), ClickHouseServerForTest.getDatabase());
                assertEquals(rs.getString("TABLE_NAME"), tableName);
                assertEquals(rs.getString("TYPE_NAME"), columnTypeNames.get(colIndex));
                assertEquals(rs.getInt("DATA_TYPE"), columnJDBCDataTypes.get(colIndex));
                assertEquals(rs.getInt("COLUMN_SIZE"), columnSizes.get(colIndex));
                assertEquals(rs.getInt("ORDINAL_POSITION"), colIndex + 1);
                assertEquals(rs.getInt("NULLABLE"), columnNullable.get(colIndex) ? DatabaseMetaData.attributeNullable : DatabaseMetaData.attributeNoNulls);
                assertEquals(rs.getString("IS_NULLABLE"), columnNullable.get(colIndex) ? "YES" : "NO");

                Integer decimalDigits = columnDecimalDigits.get(colIndex);
                if (decimalDigits != null) {
                    assertEquals(rs.getInt("DECIMAL_DIGITS"), decimalDigits.intValue());
                } else {
                    assertEquals(rs.getInt("DECIMAL_DIGITS"), 0); // should not throw exception
                    assertTrue(rs.wasNull());
                }
                Integer precisionRadix = columnRadix.get(colIndex);
                if (precisionRadix != null) {
                    assertEquals(rs.getInt("NUM_PREC_RADIX"), precisionRadix.intValue());
                } else {
                    rs.getInt("NUM_PREC_RADIX"); // should not throw exception
                    assertTrue(rs.wasNull());
                }
                count++;
            }
            Assert.assertEquals(count, columnNames.size(), "result set is empty");
        }
    }

    @Test(groups = { "integration" })
    public void testGetTables() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            List<String> expectedColumnNames = Arrays.asList(
                    "TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "TABLE_TYPE",
                    "REMARKS",
                    "TYPE_CAT",
                    "TYPE_SCHEM",
                    "TYPE_NAME",
                    "SELF_REFERENCING_COL_NAME",
                    "REF_GENERATION");

            List<Integer> expectedTableTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR);

            ResultSet rs = dbmd.getTables("system", null, "numbers", null);
            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedTableTypes);
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_NAME"), "numbers");
            assertEquals(rs.getString("TABLE_TYPE"), "SYSTEM TABLE");
            assertFalse(rs.next());
            rs.close();

            rs = dbmd.getTables("system", null, "numbers", new String[] { "SYSTEM TABLE" });
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_NAME"), "numbers");
            assertEquals(rs.getString("TABLE_TYPE"), "SYSTEM TABLE");
            assertFalse(rs.next());
            rs.close();

            rs = dbmd.getTables("system", null, "numbers", new String[] { "TABLE" });
            assertFalse(rs.next());
            rs.close();
        }
    }


    @Test(groups = { "integration" })
    public void testGetPrimaryKeys() throws Exception {
        runQuery("SELECT 1;");
        runQuery("SYSTEM FLUSH LOGS");

        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getPrimaryKeys(null, "system", "query_log");
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_NAME"), "query_log");
            assertEquals(rs.getString("COLUMN_NAME"), "event_date");
            assertEquals(rs.getShort("KEY_SEQ"), 1);
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_NAME"), "query_log");
            assertEquals(rs.getString("COLUMN_NAME"), "event_time");
            assertEquals(rs.getShort("KEY_SEQ"), 2);
            assertFalse(rs.next());
        }
    }

    @Test(groups = { "integration" })
    public void testGetSchemas() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getSchemas();
            boolean defaultSchemaFound = false;
            while (rs.next()) {
                if (rs.getString("TABLE_SCHEM").equals("default")) {
                    defaultSchemaFound = true;
                    break;
                }
            }

            assertTrue(defaultSchemaFound);
        }
    }

    @Test
    public void testSchemaTerm() throws Exception {

        try (Connection connection = getJdbcConnection()){
            Assert.assertEquals(connection.getMetaData().getSchemaTerm(), "schema");
        }

        Properties prop = new Properties();
        prop.put(DriverProperties.SCHEMA_TERM.getKey(), "database");
        try (Connection connection = getJdbcConnection(prop)){
            Assert.assertEquals(connection.getMetaData().getSchemaTerm(), "database");
        }
    }

    @Test(groups = { "integration" })
    public void testGetCatalogs() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getCatalogs();
            assertFalse(rs.next());
            ResultSetMetaDataImplTest.assertColumnNames(rs, "TABLE_CAT");
        }
    }

    @Test(groups = { "integration" })
    public void testGetTableTypes() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTableTypes();
            List<String> sortedTypes = Arrays.stream(DatabaseMetaDataImpl.TableType.values()).map(DatabaseMetaDataImpl.TableType::getTypeName).sorted().collect(Collectors.toList());
            for (String type: sortedTypes) {
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_TYPE"), type);
            }

            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetTablesReturnKnownTableTypes() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();

            try (ResultSet rs = dbmd.getTables(null, "system", "numbers", null)) {
                assertTrue(rs.next());
                String tableType = rs.getString("TABLE_TYPE");
                Assert.assertEquals(tableType, DatabaseMetaDataImpl.TableType.SYSTEM_TABLE.getTypeName(), "table was " + rs.getString("TABLE_NAME"));

            }
            try (Statement stmt = conn.createStatement()){
                stmt.executeUpdate("CREATE TABLE test_db_metadata_type_memory (v Int32) ENGINE Memory");
            }
            try (ResultSet rs = dbmd.getTables(null, "default", "test_db_metadata_type_memory", null)) {
                while (rs.next()) {
                    String tableType = rs.getString("TABLE_TYPE");
                    Assert.assertEquals(tableType, DatabaseMetaDataImpl.TableType.MEMORY_TABLE.getTypeName());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testAllTableEnginesFromSystemTableEnginesAreMapped() throws Exception {
        try (Connection conn = getJdbcConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name FROM system.table_engines ORDER BY name")) {
            Set<String> mappedEngines = DatabaseMetaDataImpl.ENGINE_TO_TABLE_TYPE.keySet();
            List<String> unmapped = new ArrayList<>();
            while (rs.next()) {
                String engine = rs.getString("name");
                boolean isMapped = mappedEngines.contains(engine)
                        || (engine != null && (engine.startsWith("System") || engine.startsWith("Async")));
                if (!isMapped) {
                    unmapped.add(engine);
                }
            }
            assertTrue(unmapped.isEmpty(),
                    "All table engines from system.table_engines must be mapped in DatabaseMetaDataImpl.ENGINE_TO_TABLE_TYPE "
                            + "or handled by System/Async prefix. Unmapped engines: " + unmapped);
        }
    }

    @Test(groups = { "integration" }, enabled = false)
    public void testGetColumnsWithEmptyCatalog() throws Exception {
        // test not relevant until catalogs are implemented
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getColumns("", null, "numbers", null);
            assertFalse(rs.next());
        }
    }

    @Test(groups = { "integration" })
    public void testGetColumnsWithEmptySchema() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getColumns("system", "", "numbers", null);
            assertFalse(rs.next());
        }
    }

    @Test(groups = { "integration" })
    public void testGetServerVersions() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            Assert.assertTrue(dbmd.getDatabaseMajorVersion() >= 21); // major version is year and cannot be less than LTS version we test with
            Assert.assertTrue(dbmd.getDatabaseMinorVersion() > 0); // minor version is always greater than 0
            Assert.assertFalse(dbmd.getDatabaseProductVersion().isEmpty(), "Version cannot be blank string");
            Assert.assertEquals(dbmd.getUserName(), "default");
        }
    }

    @Test(groups = { "integration" })
    public void testGetTypeInfo() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getTypeInfo()) {
                List<String> expectedColumnNames = Arrays.asList(
                        "TYPE_NAME",
                        "DATA_TYPE",
                        "PRECISION",
                        "LITERAL_PREFIX",
                        "LITERAL_SUFFIX",
                        "CREATE_PARAMS",
                        "NULLABLE",
                        "CASE_SENSITIVE",
                        "SEARCHABLE",
                        "UNSIGNED_ATTRIBUTE",
                        "FIXED_PREC_SCALE",
                        "AUTO_INCREMENT",
                        "LOCAL_TYPE_NAME",
                        "MINIMUM_SCALE",
                        "MAXIMUM_SCALE",
                        "SQL_DATA_TYPE",
                        "SQL_DATETIME_SUB",
                        "NUM_PREC_RADIX"
                );

                List<Integer> expectedColumnTypes = Arrays.asList(
                        Types.VARCHAR,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.SMALLINT,
                        Types.BOOLEAN,
                        Types.SMALLINT,
                        Types.BOOLEAN,
                        Types.BOOLEAN,
                        Types.BOOLEAN,
                        Types.VARCHAR,
                        Types.SMALLINT,
                        Types.SMALLINT,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.INTEGER
                );


                // check type match with
                int count = 0;
                ResultSetMetaData rsMetaData = rs.getMetaData();
                assertProcedureColumns(rsMetaData, expectedColumnNames, expectedColumnTypes);

                while (rs.next()) {
                    count++;
                    ClickHouseDataType dataType;
                    try {
                        dataType = ClickHouseDataType.of( rs.getString("TYPE_NAME"));
                    } catch (Exception e) {
                        continue; // skip. we have another test and will catch it anyway.
                    }
                    assertEquals(ClickHouseDataType.of(rs.getString(1)), dataType);
                    assertEquals(rs.getInt("DATA_TYPE"),
                            (int) JdbcUtils.convertToSqlType(dataType).getVendorTypeNumber(),
                            "Type mismatch for " + dataType.name() + ": expected " +
                                    JdbcUtils.convertToSqlType(dataType).getVendorTypeNumber() +
                                    " but was " + rs.getInt("DATA_TYPE") + " for TYPE_NAME: " + rs.getString("TYPE_NAME"));

                    assertEquals(rs.getInt("DATA_TYPE"), rs.getObject("DATA_TYPE"));

                    assertEquals(rs.getInt("PRECISION"), dataType.getMaxPrecision());
                    assertNull(rs.getString("LITERAL_PREFIX"));
                    assertNull(rs.getString("LITERAL_SUFFIX"));
                    assertEquals(rs.getInt("MINIMUM_SCALE"), dataType.getMinScale());
                    assertEquals(rs.getInt("MAXIMUM_SCALE"), dataType.getMaxScale());
                    assertNull(rs.getString("CREATE_PARAMS"));

                    if (dataType == ClickHouseDataType.Nullable || dataType == ClickHouseDataType.Dynamic) {
                        assertEquals( rs.getShort("NULLABLE"), DatabaseMetaData.typeNullable);
                    } else {
                        assertEquals(rs.getShort("NULLABLE"), DatabaseMetaData.typeNoNulls);
                    }

                    if (dataType != ClickHouseDataType.Enum && dataType != ClickHouseDataType.Geometry) {
                        assertEquals(rs.getBoolean("CASE_SENSITIVE"), dataType.isCaseSensitive());
                    }
                    assertEquals(rs.getInt("SEARCHABLE"), DatabaseMetaData.typeSearchable);
                    assertEquals(rs.getBoolean("UNSIGNED_ATTRIBUTE"), !dataType.isSigned());
                    assertFalse(rs.getBoolean("FIXED_PREC_SCALE"));
                    assertFalse(rs.getBoolean("AUTO_INCREMENT"));
                    assertEquals(rs.getString("LOCAL_TYPE_NAME"), dataType.name());
                    assertEquals(rs.getInt("MINIMUM_SCALE"), dataType.getMinScale());
                    assertEquals(rs.getInt("MAXIMUM_SCALE"), dataType.getMaxScale());
                    assertEquals(rs.getInt("SQL_DATA_TYPE"), 0);
                    assertEquals(rs.getInt("SQL_DATETIME_SUB"), 0);
                }

                assertTrue(count > 10, "At least 10 types should be returned but was " + count);
            }
        }
    }

    @Test(groups = {"integration"})
    public void testFindNestedTypes() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getTypeInfo()) {
                Set<String> nestedTypes = Arrays.stream(ClickHouseDataType.values())
                        .filter(ClickHouseDataType::isNested).map(Enum::name).collect(Collectors.toSet());

                while (rs.next()) {
                    String typeName = rs.getString("TYPE_NAME");
                    nestedTypes.remove(typeName);
                }

                if (ClickHouseVersion.of(getServerVersion()).check("(,25.10]")) {
                    assertEquals(nestedTypes, Arrays.asList("Geometry")); // Geometry was introduced in 25.11
                } else {
                    assertEquals(nestedTypes, Arrays.asList("Object")); // Object is deprecated in 25.11
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetFunctions() throws Exception {
        if (ClickHouseVersion.of(getServerVersion()).check("(,23.8]")) {
            return; //  Illegal column Int8 of argument of function concat. (ILLEGAL_COLUMN)  TODO: fix in JDBC
        }

        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getFunctions(null, null, "mapContains")) {

                List<String> expectedColumnNames = Arrays.asList(
                        "FUNCTION_CAT",
                        "FUNCTION_SCHEM",
                        "FUNCTION_NAME",
                        "REMARKS",
                        "FUNCTION_TYPE",
                        "SPECIFIC_NAME"
                );

                List<Integer> expectedColumnTypes = Arrays.asList(
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.SMALLINT,
                        Types.VARCHAR
                );

                assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);

                assertTrue(rs.next());
                assertNull(rs.getString("FUNCTION_CAT"));
                assertNull(rs.getString("FUNCTION_SCHEM"));
                assertEquals(rs.getString("FUNCTION_NAME"), "mapContains");
                assertFalse(rs.getString("REMARKS").isEmpty());
                assertEquals(rs.getShort("FUNCTION_TYPE"), DatabaseMetaData.functionResultUnknown);
                assertEquals(rs.getString("SPECIFIC_NAME"), "mapContains");
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetFunctionColumns() throws Exception {
        if (ClickHouseVersion.of(getServerVersion()).check("(,23.8]")) {
            return; //  Illegal column Int8 of argument of function concat. (ILLEGAL_COLUMN)  TODO: fix in JDBC
        }

        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getFunctionColumns(null, null, "mapContains", null)) {
                assertFalse(rs.next());
                List<String> expectedColumnNames = Arrays.asList(
                        "FUNCTION_CAT",
                        "FUNCTION_SCHEM",
                        "FUNCTION_NAME",
                        "COLUMN_NAME",
                        "COLUMN_TYPE",
                        "DATA_TYPE",
                        "TYPE_NAME",
                        "PRECISION",
                        "LENGTH",
                        "SCALE",
                        "RADIX",
                        "NULLABLE",
                        "REMARKS",
                        "CHAR_OCTET_LENGTH",
                        "ORDINAL_POSITION",
                        "IS_NULLABLE",
                        "SPECIFIC_NAME"
                );

                List<Integer> expectedColumnTypes = Arrays.asList(
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.SMALLINT,
                        Types.INTEGER,
                        Types.VARCHAR,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.SMALLINT,
                        Types.SMALLINT,
                        Types.SMALLINT,
                        Types.VARCHAR,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.VARCHAR,
                        Types.VARCHAR
                );

                assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);

            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetClientInfoProperties() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getClientInfoProperties()) {
                for (ClientInfoProperties p : ClientInfoProperties.values()) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(rs.getString("NAME"), p.getKey());
                    Assert.assertEquals(rs.getInt("MAX_LEN"), p.getMaxValue());
                    Assert.assertEquals(rs.getString("DEFAULT_VALUE"), p.getDefaultValue());
                    Assert.assertEquals(rs.getString("DESCRIPTION"), p.getDescription());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetDriverVersion() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            String version = dbmd.getDriverVersion();
            assertNotEquals(version, "unknown");
        }
    }


    @Test(groups = {"integration"})
    public void testGetIndexInfoColumnType() throws Exception {

        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();

            List<String> expectedColumnNames = Arrays.asList("TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "NON_UNIQUE",
                    "INDEX_QUALIFIER",
                    "INDEX_NAME",
                    "TYPE",
                    "ORDINAL_POSITION",
                    "COLUMN_NAME",
                    "ASC_OR_DESC",
                    "CARDINALITY",
                    "PAGES",
                    "FILTER_CONDITION");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.BOOLEAN,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.BIGINT,
                    Types.BIGINT,
                    Types.VARCHAR
            );

            ResultSet rs = dbmd.getIndexInfo(null, null, null, false, false);
            assertFalse(rs.next());
            ResultSetMetaData rsmd = rs.getMetaData();
            assertProcedureColumns(rsmd, expectedColumnNames, expectedColumnTypes);
        }
    }

    @Test(groups = {"integration"})
    public void testGetProcedures() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            List<String> columnNames = Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "RESERVED1",
                    "RESERVED2", "RESERVED3", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME");
            List<Integer> columnTypes = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT,
                    Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.SMALLINT, Types.VARCHAR);

            ResultSet rs = dbmd.getProcedures(null, null, null);
            assertFalse(rs.next());
            ResultSetMetaData rsmd = rs.getMetaData();
            assertProcedureColumns(rsmd, columnNames, columnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetProceduresColumnType() throws Exception {

        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();

            List<String> expectedColumnNames = Arrays.asList("PROCEDURE_CAT",
                    "PROCEDURE_SCHEM",
                    "PROCEDURE_NAME",
                    "COLUMN_NAME",
                    "COLUMN_TYPE",
                    "DATA_TYPE",
                    "TYPE_NAME",
                    "PRECISION",
                    "LENGTH",
                    "SCALE",
                    "RADIX",
                    "NULLABLE",
                    "REMARKS",
                    "COLUMN_DEF",
                    "SQL_DATA_TYPE",
                    "SQL_DATETIME_SUB",
                    "CHAR_OCTET_LENGTH",
                    "ORDINAL_POSITION",
                    "IS_NULLABLE",
                    "SPECIFIC_NAME");
            List<Integer> columnTypes = Arrays.asList(Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.VARCHAR);
            ResultSetMetaData rsmd = dbmd.getProcedureColumns(null, null, null, null).getMetaData();

            assertProcedureColumns(rsmd, expectedColumnNames, columnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetColumnPrivileges() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getColumnPrivileges(null, null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "GRANTOR",
                    "GRANTEE",
                    "PRIVILEGE",
                    "IS_GRANTABLE");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetTablePrivileges() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTablePrivileges(null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "GRANTOR",
                    "GRANTEE",
                    "PRIVILEGE",
                    "IS_GRANTABLE");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }

    @Test(groups = {"integration"})
    public void testGetVersionColumnsColumns() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getVersionColumns(null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("SCOPE",
                    "COLUMN_NAME",
                    "DATA_TYPE",
                    "TYPE_NAME",
                    "COLUMN_SIZE",
                    "BUFFER_LENGTH",
                    "DECIMAL_DIGITS",
                    "PSEUDO_COLUMN");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.SMALLINT,
                    Types.SMALLINT
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetPrimaryKeysColumns() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getPrimaryKeys(null, null, null);

            List<String> expectedColumnNames = Arrays.asList("TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "KEY_SEQ",
                    "PK_NAME");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.VARCHAR
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }

    @Test(groups = {"integration"})
    public void testGetImportedKeys() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getImportedKeys(null, null, null);
            assertFalse(rs.next());

            List<String> expectedColumnNames = Arrays.asList(
                    "PKTABLE_CAT",
                    "PKTABLE_SCHEM",
                    "PKTABLE_NAME",
                    "PKCOLUMN_NAME",
                    "FKTABLE_CAT",
                    "FKTABLE_SCHEM",
                    "FKTABLE_NAME",
                    "FKCOLUMN_NAME",
                    "KEY_SEQ",
                    "UPDATE_RULE",
                    "DELETE_RULE",
                    "FK_NAME",
                    "PK_NAME",
                    "DEFERRABILITY");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }

    @Test(groups = {"integration"})
    public void testGetExportedKeys() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getExportedKeys(null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("PKTABLE_CAT",
                    "PKTABLE_SCHEM",
                    "PKTABLE_NAME",
                    "PKCOLUMN_NAME",
                    "FKTABLE_CAT",
                    "FKTABLE_SCHEM",
                    "FKTABLE_NAME",
                    "FKCOLUMN_NAME",
                    "KEY_SEQ",
                    "UPDATE_RULE",
                    "DELETE_RULE",
                    "FK_NAME",
                    "PK_NAME",
                    "DEFERRABILITY");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetCrossReference() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getCrossReference(null, null, null, null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("PKTABLE_CAT",
                    "PKTABLE_SCHEM",
                    "PKTABLE_NAME",
                    "PKCOLUMN_NAME",
                    "FKTABLE_CAT",
                    "FKTABLE_SCHEM",
                    "FKTABLE_NAME",
                    "FKCOLUMN_NAME",
                    "KEY_SEQ",
                    "UPDATE_RULE",
                    "DELETE_RULE",
                    "FK_NAME",
                    "PK_NAME",
                    "DEFERRABILITY");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }

    @Test(groups = {"integration"})
    public void testGetUDTs() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getUDTs(null, null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("TYPE_CAT",
                    "TYPE_SCHEM",
                    "TYPE_NAME",
                    "CLASS_NAME",
                    "DATA_TYPE",
                    "REMARKS",
                    "BASE_TYPE");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.SMALLINT
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetSuperTypes() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getSuperTypes(null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("TYPE_CAT",
                    "TYPE_SCHEM",
                    "TYPE_NAME",
                    "SUPERTYPE_CAT",
                    "SUPERTYPE_SCHEM",
                    "SUPERTYPE_NAME");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }

    @Test(groups = {"integration"})
    public void testGetSuperTables() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getSuperTables(null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "SUPERTABLE_NAME");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetBestRowIdentifier() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getBestRowIdentifier(null, null, null, 0, true);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("SCOPE",
                    "COLUMN_NAME",
                    "DATA_TYPE",
                    "TYPE_NAME",
                    "COLUMN_SIZE",
                    "BUFFER_LENGTH",
                    "DECIMAL_DIGITS",
                    "PSEUDO_COLUMN");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.SMALLINT,
                    Types.SMALLINT
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetAttributes() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getAttributes(null, null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("TYPE_CAT",
                    "TYPE_SCHEM",
                    "TYPE_NAME",
                    "ATTR_NAME",
                    "DATA_TYPE",
                    "ATTR_TYPE_NAME",
                    "ATTR_SIZE",
                    "DECIMAL_DIGITS",
                    "NUM_PREC_RADIX",
                    "NULLABLE",
                    "REMARKS",
                    "ATTR_DEF",
                    "SQL_DATA_TYPE",
                    "SQL_DATETIME_SUB",
                    "CHAR_OCTET_LENGTH",
                    "ORDINAL_POSITION",
                    "IS_NULLABLE",
                    "SCOPE_CATALOG",
                    "SCOPE_SCHEMA",
                    "SCOPE_TABLE",
                    "SOURCE_DATA_TYPE");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetPseudoColumns() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getPseudoColumns(null, null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "DATA_TYPE",
                    "COLUMN_SIZE",
                    "DECIMAL_DIGITS",
                    "NUM_PREC_RADIX",
                    "COLUMN_USAGE",
                    "REMARKS",
                    "CHAR_OCTET_LENGTH",
                    "IS_NULLABLE");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.VARCHAR
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }

    private void assertProcedureColumns(ResultSetMetaData rsmd, List<String> expectedColumnNames, List<Integer> expectedColumnTypes) throws SQLException {
        int columnCount = rsmd.getColumnCount();
        assertEquals(columnCount, expectedColumnNames.size(), "number of columns");
        for (int i = 1; i <= columnCount; i++) {
            String columnName = rsmd.getColumnName(i);
            int columnType = rsmd.getColumnType(i);
            assertEquals(columnName, expectedColumnNames.get(i - 1), "Column name mismatch");
            assertEquals(columnType, expectedColumnTypes.get(i - 1), "Column type mismatch for column name " + columnName + " (" + i + ")");
        }
    }



    @Test(groups = {"integration"})
    public void testGetDatabaseMajorVersion() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int majorVersion = dbmd.getDatabaseMajorVersion();
            String version =  getServerVersion();
            int majorVersionOfServer = Integer.parseInt(version.split("\\.")[0]);
            assertEquals(majorVersion, majorVersionOfServer, "Major version");
        }
    }


    @Test(groups = {"integration"})
    public void testGetDatabaseMinorVersion() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int minorVersion = dbmd.getDatabaseMinorVersion();
            String version =  getServerVersion();
            int minorVersionOfServer = Integer.parseInt(version.split("\\.")[1]);
            assertEquals(minorVersion, minorVersionOfServer, "Minor version");
        }
    }

    @Test(groups = {"integration"})
    public void testTableTypes() throws Exception {
        final String database = getDatabase();

        // Map of table name -> (schema, expected type), ordered alphabetically by type
        java.util.Map<String, String[]> knownTables = new java.util.LinkedHashMap<>();
        knownTables.put("test_table_types_dict", new String[]{database, "DICTIONARY"});
        knownTables.put("test_table_types_mat_view", new String[]{database, "MATERIALIZED VIEW"});
        knownTables.put("test_table_types_remote", new String[]{database, "REMOTE TABLE"});
        knownTables.put("numbers", new String[]{"system", "SYSTEM TABLE"});
        knownTables.put("test_table_types_regular", new String[]{database, "TABLE"});
        knownTables.put("test_table_types_view", new String[]{database, "VIEW"});
        if (!isCloud()) {
            knownTables.put("test_table_types_log", new String[]{database, "LOG TABLE"});
            knownTables.put("test_table_types_memory", new String[]{database, "MEMORY TABLE"});
        }

        try (Connection conn = getJdbcConnection()) {
            final DatabaseMetaData dbmd = conn.getMetaData();

            try (Statement stmt = conn.createStatement()) {
                // Regular MergeTree table
                stmt.executeUpdate("DROP TABLE IF EXISTS test_table_types_regular");
                stmt.executeUpdate("CREATE TABLE test_table_types_regular (id Int32) ENGINE = MergeTree ORDER BY id");

                // Source table for views
                stmt.executeUpdate("DROP TABLE IF EXISTS test_table_types_source");
                stmt.executeUpdate("CREATE TABLE test_table_types_source (id Int32) ENGINE = MergeTree ORDER BY id");

                // Normal view
                stmt.executeUpdate("DROP VIEW IF EXISTS test_table_types_view");
                stmt.executeUpdate("CREATE VIEW test_table_types_view AS SELECT id FROM test_table_types_source");

                // Materialized view
                stmt.executeUpdate("DROP VIEW IF EXISTS test_table_types_mat_view");
                stmt.executeUpdate("CREATE MATERIALIZED VIEW test_table_types_mat_view ENGINE = MergeTree ORDER BY id AS SELECT id FROM test_table_types_source");

                // Remote table (URL engine has empty data_paths)
                stmt.executeUpdate("DROP TABLE IF EXISTS test_table_types_remote");
                stmt.executeUpdate("CREATE TABLE test_table_types_remote (id Int32) ENGINE = URL('http://localhost:8123/?query=SELECT+1', CSV)");

                // Log table
                if (!isCloud()) {
                    // not supported by cloud https://clickhouse.com/docs/engines/table-engines/log-family
                    stmt.executeUpdate("DROP TABLE IF EXISTS test_table_types_log");
                    stmt.executeUpdate("CREATE TABLE test_table_types_log (id Int32) ENGINE = Log");
                }


                // Memory table
                if (!isCloud()) {
                    // memory table is not replicated across cluster so need persistent connection - useless with http protocol
                    stmt.executeUpdate("DROP TABLE IF EXISTS test_table_types_memory");
                    stmt.executeUpdate("CREATE TABLE test_table_types_memory (id Int32) ENGINE = Memory");
                }

                // Dictionary (using source table as source)
                stmt.executeUpdate("DROP DICTIONARY IF EXISTS test_table_types_dict");
                stmt.executeUpdate("CREATE DICTIONARY test_table_types_dict (id Int32) " +
                        "PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'test_table_types_source' DB '" + database + "')) " +
                        "LAYOUT(FLAT()) LIFETIME(0)");

            }

            // Get all types from DatabaseMetaData and verify against expected list
            Set<String> allTypes = new HashSet<>();
            try (ResultSet rs = dbmd.getTableTypes()) {
                while (rs.next()) {
                    allTypes.add(rs.getString("TABLE_TYPE"));
                }
            }
            final Set<String> expectedTypes = new HashSet<>(Arrays.asList(
                    "DICTIONARY",
                    "LOG TABLE",
                    "MATERIALIZED VIEW",
                    "MEMORY TABLE",
                    "REMOTE TABLE",
                    "SYSTEM TABLE",
                    "TABLE",
                    "TEMPORARY TABLE",  // Temporary tables are visible only in the session where they created.
                    "VIEW"
            ));
            assertEquals(allTypes, expectedTypes, "Table types from getTableTypes() should match expected types");

            for (java.util.Map.Entry<String, String[]> entry : knownTables.entrySet()) {
                String tableName = entry.getKey();
                String schema = entry.getValue()[0];
                String expectedType = entry.getValue()[1];

                // Test with no filter - table should be returned
                try (ResultSet rs = dbmd.getTables(null, schema, tableName, null)) {
                    assertTrue(rs.next(), tableName + " should be found with no filter");
                    assertEquals(rs.getString("TABLE_NAME"), tableName);
                    assertEquals(rs.getString("TABLE_TYPE"), expectedType);
                    assertFalse(rs.next());
                }

                // Test with each type filter - table should be returned only when filter matches
                for (String filterType : allTypes) {
                    try (ResultSet rs = dbmd.getTables(null, schema, tableName, new String[]{filterType})) {
                        if (filterType.equals(expectedType)) {
                            assertTrue(rs.next(), tableName + " should be found with matching filter " + filterType);
                            assertEquals(rs.getString("TABLE_NAME"), tableName);
                            assertEquals(rs.getString("TABLE_TYPE"), expectedType);
                            assertFalse(rs.next());
                        } else {
                            assertFalse(rs.next(), tableName + " should NOT be found with filter " + filterType);
                        }
                    }
                }
            }
        }
    }
}
