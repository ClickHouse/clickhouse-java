package ru.yandex.metrika.clickhouse;

import ru.yandex.metrika.clickhouse.copypaste.CHResultBuilder;
import ru.yandex.metrika.clickhouse.copypaste.CHResultSet;
import ru.yandex.metrika.clickhouse.util.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jkee on 14.03.15.
 */
public class CHDatabaseMetadata implements DatabaseMetaData {

    private static final Logger log = Logger.of(CHDatabaseMetadata.class);

    private String url;
    private CHConnection connection;

    public CHDatabaseMetadata(String url, CHConnection connection) {
        this.url = url;
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
        return url;
    }

    @Override
    public String getUserName() throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
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
        return true;
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
        return "0.42";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "ru.yandex.metrika.clickhouse-jdbc";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "0.1";
    }

    @Override
    public int getDriverMajorVersion() {
        return 0;
    }

    @Override
    public int getDriverMinorVersion() {
        return 1;
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
        return true;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return true;
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
        return true;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return true;
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
        return "GLOBAL,ARRAY";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
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
        return false;
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
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
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
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "some bullshit";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "database";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ":";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
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
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
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
        return false;
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
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level == Connection.TRANSACTION_NONE;
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

    private ResultSet request(String sql) throws SQLException {
        Statement statement = connection.createStatement();
        return statement.executeQuery(sql);
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        CHResultBuilder builder = CHResultBuilder.builder(9);
        builder.names(
                "PROCEDURE_CAT",
                "PROCEDURE_SCHEM",
                "PROCEDURE_NAME",
                "RES_1",
                "RES_2",
                "RES_3",
                "REMARKS",
                "PROCEDURE_TYPE",
                "SPECIFIC_NAME"
        );

        builder.types(
                "String",
                "String",
                "String",
                "String",
                "String",
                "String",
                "String",
                "UInt8",
                "String"
        );

        return builder.build();
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        CHResultBuilder builder = CHResultBuilder.builder(20);
        builder.names("1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20");

        builder.types("UInt32","UInt32","UInt32","UInt32","UInt32","UInt32","UInt32","UInt32","UInt32","UInt32","UInt32","UInt32","UInt32","UInt32","UInt32","UInt32","UInt32","UInt32","UInt32","UInt32");

        return builder.build();
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        /**
         TABLE_CAT String => table catalog (may be null)
         TABLE_SCHEM String => table schema (may be null)
         TABLE_NAME String => table name
         TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
         REMARKS String => explanatory comment on the table
         TYPE_CAT String => the types catalog (may be null)
         TYPE_SCHEM String => the types schema (may be null)
         TYPE_NAME String => type name (may be null)
         SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
         REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)
         */
        if (catalog == null || catalog.isEmpty()) {
            catalog = "default";
        }
        String sql = "select " +
                "database, name, name " +
                "from system.tables " +
                "where database = '" + catalog + "' " +
                "order by name";
        ResultSet result = request(sql);

        CHResultBuilder builder = CHResultBuilder.builder(10);
        builder.names("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION");
        builder.types("String", "String", "String", "String", "String", "String", "String", "String", "String", "String");

        while(result.next()) {
            List<String> row = new ArrayList<String>();
            row.add(result.getString(1));
            row.add(result.getString(2));
            row.add(result.getString(3));
            row.add("TABLE"); // можно сделать точнее
            for (int i = 4; i < 10; i++) {
                row.add(null);
            }
            builder.addRow(row);
        }
        return builder.build();
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        String sql = "select name, database from system.tables";
        if (catalog != null) sql += " where database = '" + catalog + '\'';
        if (schemaPattern != null) {
            if (catalog != null) sql += " and ";
            else sql += " where ";
            sql += "name = '" + schemaPattern + '\'';
        }
        return request(sql);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return request("show databases");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return request("select 'TABLE', 'LOCAL TEMPORARY'");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        CHResultBuilder builder = CHResultBuilder.builder(23);
        builder.names(
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
                "SCOPE_CATLOG",
                "SCOPE_SCHEMA",
                "SCOPE_TABLE",
                "SOURCE_DATA_TYPE",
                "IS_AUTOINCREMENT"
        );
        builder.types(
                "String",
                "String",
                "String",
                "String",
                "Int32",
                "String",
                "Int32",
                "Int32",
                "Int32",
                "Int32",
                "Int32",
                "String",
                "String",
                "Int32",
                "Int32",
                "Int32",
                "Int32",
                "String",
                "String",
                "String",
                "String",
                "Int32",
                "String"
        );
        ResultSet descTable = request("desc table " + catalog + '.' + tableNamePattern);
        int colNum = 1;
        while (descTable.next()) {
            List<String> row = new ArrayList<String>();
            row.add(catalog);
            row.add(tableNamePattern);
            row.add(tableNamePattern);
            row.add(descTable.getString(1));
            String type = descTable.getString(2);
            row.add(Integer.toString(CHResultSet.toSqlType(type)));
            row.add(type);

            // column size ?
            row.add("0");
            row.add("0");

            // decimal digits
            if (type.contains("Int")) {
                String bits = type.substring(type.indexOf("Int") + "Int".length());
                row.add(bits); //bullshit
            } else {
                row.add(null);
            }

            // radix
            row.add("10");
            // nullable
            row.add(String.valueOf(columnNoNulls));

            row.add(null);
            row.add(null);
            row.add(null);
            row.add(null);

            // char octet length
            row.add("0");
            // ordinal
            row.add(String.valueOf(colNum));
            colNum += 1;
            row.add("NO");

            row.add(null);
            row.add(null);
            row.add(null);
            row.add(null);
            row.add(null);

            builder.addRow(row);

        }

        return builder.build();
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        CHResultBuilder builder = CHResultBuilder.builder(18);
        builder.names(
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
        builder.types(
                "String",
                "Int32",
                "Int32",
                "String",
                "String",
                "String",
                "Int32",
                "Int8",
                "Int32",
                "Int8",
                "Int8",
                "Int8",
                "String",
                "Int32",
                "Int32",
                "Int32",
                "Int32",
                "Int32"
        );
        builder.addRow(
                "String", Types.VARCHAR,
                null, // precision - todo
                '\'', '\'', null,
                typeNoNulls, true, typeSearchable,
                true, // unsigned
                true, // fixed precision (money)
                false, //auto-incr
                null,
                null, null, // scale - should be fixed
                null, null,
                10
                );
        int[] sizes = { 8, 16, 32, 64 };
        boolean[] signed = { true, false };
        for (int size : sizes) {
            for (boolean b: signed) {
                String name = (b ? "" : "U") + "Int" + size;
                builder.addRow(
                        name, (size <= 16 ? Types.INTEGER : Types.BIGINT),
                        null, // precision - todo
                        null, null, null,
                        typeNoNulls, true, typePredBasic,
                        !b, // unsigned
                        true, // fixed precision (money)
                        false, //auto-incr
                        null,
                        null, null, // scale - should be fixed
                        null, null,
                        10
                );
            }
        }
        int[] floatSizes = { 32, 64 };
        for (int floatSize : floatSizes) {
            String name = "Float" + floatSize;
            builder.addRow(
                    name, Types.FLOAT,
                    null, // precision - todo
                    null, null, null,
                    typeNoNulls, true, typePredBasic,
                    false, // unsigned
                    true, // fixed precision (money)
                    false, //auto-incr
                    null,
                    null, null, // scale - should be fixed
                    null, null,
                    10
            );
        }
        builder.addRow(
                "Date", Types.DATE,
                null, // precision - todo
                null, null, null,
                typeNoNulls, true, typePredBasic,
                false, // unsigned
                true, // fixed precision (money)
                false, //auto-incr
                null,
                null, null, // scale - should be fixed
                null, null,
                10
        );
        builder.addRow(
                "DateTime", Types.TIMESTAMP,
                null, // precision - todo
                null, null, null,
                typeNoNulls, true, typePredBasic,
                false, // unsigned
                true, // fixed precision (money)
                false, //auto-incr
                null,
                null, null, // scale - should be fixed
                null, null,
                10
        );
        return builder.build();
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        int[] types = CHResultSet.supportedTypes();
        for (int i : types) {
            if (i == type) return true;
        }
        return false;
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
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return null;
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
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 1;
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
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
