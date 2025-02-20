package com.clickhouse.jdbc;

import java.sql.DriverPropertyInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

/**
 * JDBC-specific configuration.
 */
@Deprecated
public class JdbcConfig {
    private static final Logger log = LoggerFactory.getLogger(JdbcConfig.class);

    public static final String PROP_AUTO_COMMIT = "autoCommit";
    public static final String PROP_CREATE_DATABASE = "createDatabaseIfNotExist";
    public static final String PROP_CONTINUE_BATCH = "continueBatchOnError";
    public static final String PROP_DATABASE_TERM = "databaseTerm";
    public static final String PROP_DIALECT = "dialect";
    public static final String PROP_EXTERNAL_DATABASE = "externalDatabase";
    public static final String PROP_FETCH_SIZE = "fetchSize";
    public static final String PROP_LOCAL_FILE = "localFile";
    public static final String PROP_JDBC_COMPLIANT = "jdbcCompliant";
    public static final String PROP_NAMED_PARAM = "namedParameter";
    public static final String PROP_NULL_AS_DEFAULT = "nullAsDefault";
    public static final String PROP_TX_SUPPORT = "transactionSupport";
    public static final String PROP_TYPE_MAP = "typeMappings";
    public static final String PROP_WRAPPER_OBJ = "wrapperObject";

    static final String TERM_COMMENT = "comment";
    static final String TERM_DATABASE = "database";
    static final String TERM_TABLE = "table";
    static final String TERM_CATALOG = "catalog";
    static final String TERM_SCHEMA = "schema";

    private static final String BOOLEAN_FALSE = "false";
    private static final String BOOLEAN_TRUE = "true";

    private static final String DEFAULT_AUTO_COMMIT = BOOLEAN_TRUE;
    private static final String DEFAULT_CREATE_DATABASE = BOOLEAN_FALSE;
    private static final String DEFAULT_CONTINUE_BATCH = BOOLEAN_FALSE;
    private static final String DEFAULT_DATABASE_TERM = TERM_CATALOG;
    private static final String DEFAULT_DIALECT = "";
    private static final String DEFAULT_EXTERNAL_DATABASE = BOOLEAN_TRUE;
    private static final String DEFAULT_FETCH_SIZE = "0";
    private static final String DEFAULT_LOCAL_FILE = BOOLEAN_FALSE;
    private static final String DEFAULT_JDBC_COMPLIANT = BOOLEAN_TRUE;
    private static final String DEFAULT_NAMED_PARAM = BOOLEAN_FALSE;
    private static final String DEFAULT_NULL_AS_DEFAULT = "0";
    private static final String DEFAULT_TX_SUPPORT = BOOLEAN_FALSE;
    private static final String DEFAULT_TYPE_MAP = "";
    private static final String DEFAULT_WRAPPER_OBJ = BOOLEAN_FALSE;

    static DriverPropertyInfo newDriverProperty(String name, String defaultValue, String description,
            String... choices) {
        DriverPropertyInfo info = new DriverPropertyInfo(name, defaultValue);
        info.description = description;
        if (choices != null && choices.length > 0) {
            info.choices = choices;
        }
        return info;
    }

    public static List<DriverPropertyInfo> getDriverProperties() {
        return Collections.unmodifiableList(new ArrayList<>(Arrays.asList(
                newDriverProperty(PROP_AUTO_COMMIT, DEFAULT_AUTO_COMMIT,
                        "Whether to enable auto commit when connection is created."),
                newDriverProperty(PROP_CREATE_DATABASE, DEFAULT_CREATE_DATABASE,
                        "Whether to automatically create database when it does not exist.", BOOLEAN_TRUE,
                        BOOLEAN_FALSE),
                newDriverProperty(PROP_CONTINUE_BATCH, DEFAULT_CONTINUE_BATCH,
                        "Whether to continue batch process when error occurred.", BOOLEAN_TRUE, BOOLEAN_FALSE),
                newDriverProperty(PROP_FETCH_SIZE, DEFAULT_FETCH_SIZE,
                        "Default fetch size, negative or zero means no preferred option."),
                newDriverProperty(PROP_LOCAL_FILE, DEFAULT_LOCAL_FILE,
                        "Whether to use local file for INFILE/OUTFILE or not.", BOOLEAN_TRUE, BOOLEAN_FALSE),
                newDriverProperty(PROP_JDBC_COMPLIANT, DEFAULT_JDBC_COMPLIANT,
                        "Whether to enable JDBC-compliant features like fake transaction and standard UPDATE and DELETE statements.",
                        BOOLEAN_TRUE, BOOLEAN_FALSE),
                newDriverProperty(PROP_DATABASE_TERM, DEFAULT_DATABASE_TERM,
                        "Default JDBC term as synonymous to database.", TERM_CATALOG, TERM_SCHEMA),
                newDriverProperty(PROP_DIALECT, DEFAULT_DIALECT,
                        "Dialect mainly for data type mapping, can be set to ansi or a full qualified class name implementing JdbcTypeMapping."),
                newDriverProperty(PROP_EXTERNAL_DATABASE, DEFAULT_EXTERNAL_DATABASE,
                        "Whether to enable external database support or not.", BOOLEAN_TRUE, BOOLEAN_FALSE),
                newDriverProperty(PROP_NAMED_PARAM, DEFAULT_NAMED_PARAM,
                        "Whether to use named parameter(e.g. :ts(DateTime64(6)) or :value etc.) instead of standard JDBC question mark placeholder.",
                        BOOLEAN_TRUE, BOOLEAN_FALSE),
                newDriverProperty(PROP_NULL_AS_DEFAULT, DEFAULT_NULL_AS_DEFAULT,
                        "Default approach to handle null value, sets to 0 or negative number to throw exception when target column is not nullable, 1 to disable the null-check, and 2 or higher to replace null to default value of corresponding data type."),
                newDriverProperty(PROP_TX_SUPPORT, DEFAULT_TX_SUPPORT, "Whether to enable transaction support or not.",
                        BOOLEAN_TRUE, BOOLEAN_FALSE),
                newDriverProperty(PROP_TYPE_MAP, DEFAULT_TYPE_MAP,
                        "Default type mappings between ClickHouse data type and Java class. You can define multiple mappings using comma as separator."),
                newDriverProperty(PROP_WRAPPER_OBJ, DEFAULT_WRAPPER_OBJ,
                        "Whether to return wrapper object like Array or Struct in ResultSet.getObject method.",
                        BOOLEAN_TRUE, BOOLEAN_FALSE))));
    }

    String removeAndGetPropertyValue(Properties props, String key) {
        if (props == null || props.isEmpty() || key == null || key.isEmpty()) {
            return null;
        }

        // Remove JDBC-specific options so that they won't be treated as server settings
        // at later stage. Default properties won't be used for the same reason.
        Object raw = props.remove(key);
        if (raw != null) {
            this.properties.put(key, raw);
            return raw.toString();
        } else {
            return null;
        }
    }

    boolean extractBooleanValue(Properties props, String key, String defaultValue) {
        String value = removeAndGetPropertyValue(props, key);
        return Boolean.parseBoolean(value != null ? value : defaultValue);
    }

    int extractIntValue(Properties props, String key, String defaultValue) {
        String value = removeAndGetPropertyValue(props, key);
        return Integer.parseInt(value != null ? value : defaultValue);
    }

    // TODO return JdbcDialect
    JdbcTypeMapping extractDialectValue(Properties props, String key, String defaultValue) {
        String value = removeAndGetPropertyValue(props, key);
        if (value == null) {
            value = defaultValue;
        }

        JdbcTypeMapping mapper;
        if (ClickHouseChecker.isNullOrBlank(value)) {
            mapper = JdbcTypeMapping.getDefaultMapping();
        } else if ("ansi".equalsIgnoreCase(value)) {
            mapper = JdbcTypeMapping.getAnsiMapping();
        } else {
            try {
                Class<?> clazz = JdbcConfig.class.getClassLoader().loadClass(value);
                mapper = (JdbcTypeMapping) clazz.getConstructor().newInstance();
            } catch (Throwable t) {
                log.warn("Failed to load custom JDBC type mapping [%s], due to: %s", value, t.getMessage());
                mapper = JdbcTypeMapping.getDefaultMapping();
            }
        }
        return mapper;
    }

    String extractStringValue(Properties props, String key, String defaultValue) {
        String value = removeAndGetPropertyValue(props, key);
        return value != null ? value : defaultValue;
    }

    Map<String, Class<?>> extractTypeMapValue(Properties props, String key, String defaultValue) {
        String value = removeAndGetPropertyValue(props, key);
        if (value == null) {
            value = defaultValue;
        }

        if (ClickHouseChecker.isNullOrBlank(value)) {
            return Collections.emptyMap();
        }

        Map<String, Class<?>> map = new LinkedHashMap<>();
        ClassLoader loader = JdbcConfig.class.getClassLoader();
        for (Entry<String, String> e : ClickHouseOption.toKeyValuePairs(value).entrySet()) {
            Class<?> clazz = null;
            try {
                clazz = loader.loadClass(e.getValue());
            } catch (Throwable t) {
                log.warn("Failed to add mapping [%s]=[%s], due to: %s", e.getKey(), e.getValue(), t.getMessage());
            }
            if (clazz != null) {
                map.put(e.getKey(), clazz);
            }
        }

        return Collections.unmodifiableMap(map);
    }

    private final Properties properties;

    private final boolean autoCommit;
    private final boolean createDb;
    private final boolean continueBatch;
    private final int fetchSize;
    private final boolean localFile;
    private final boolean jdbcCompliant;
    private final String databaseTerm;
    private final JdbcTypeMapping dialect;
    private final boolean externalDatabase;
    private final boolean namedParameter;
    private final int nullAsDefault;
    private final boolean txSupport;
    private final Map<String, Class<?>> typeMap;
    private final boolean wrapperObject;

    public JdbcConfig() {
        this(null);
    }

    public JdbcConfig(Properties props) {
        this.properties = new Properties();

        this.autoCommit = extractBooleanValue(props, PROP_AUTO_COMMIT, DEFAULT_AUTO_COMMIT);
        this.createDb = extractBooleanValue(props, PROP_CREATE_DATABASE, DEFAULT_CREATE_DATABASE);
        this.continueBatch = extractBooleanValue(props, PROP_CONTINUE_BATCH, DEFAULT_CONTINUE_BATCH);
        this.databaseTerm = extractStringValue(props, PROP_DATABASE_TERM, DEFAULT_DATABASE_TERM);
        this.dialect = extractDialectValue(props, PROP_DIALECT, DEFAULT_DIALECT);
        this.externalDatabase = extractBooleanValue(props, PROP_EXTERNAL_DATABASE, DEFAULT_EXTERNAL_DATABASE);
        this.fetchSize = extractIntValue(props, PROP_FETCH_SIZE, DEFAULT_FETCH_SIZE);
        this.localFile = extractBooleanValue(props, PROP_LOCAL_FILE, DEFAULT_LOCAL_FILE);
        this.jdbcCompliant = extractBooleanValue(props, PROP_JDBC_COMPLIANT, DEFAULT_JDBC_COMPLIANT);
        this.namedParameter = extractBooleanValue(props, PROP_NAMED_PARAM, DEFAULT_NAMED_PARAM);
        this.nullAsDefault = extractIntValue(props, PROP_NULL_AS_DEFAULT, DEFAULT_NULL_AS_DEFAULT);
        this.txSupport = extractBooleanValue(props, PROP_TX_SUPPORT, DEFAULT_TX_SUPPORT);
        this.typeMap = extractTypeMapValue(props, PROP_TYPE_MAP, DEFAULT_TYPE_MAP);
        this.wrapperObject = extractBooleanValue(props, PROP_WRAPPER_OBJ, DEFAULT_WRAPPER_OBJ);
    }

    /**
     * Checks whether auto commit should be enabled when creating a connection.
     *
     * @return true if auto commit should be enabled when creating connection; false
     *         otherwise
     */
    public boolean isAutoCommit() {
        return autoCommit;
    }

    /**
     * Checks whether database should be created automatically when it does not
     * exist.
     *
     * @return true if database should be created automatically; false otherwise
     */
    public boolean isCreateDbIfNotExist() {
        return createDb;
    }

    /**
     * Checks whether batch processing should continue when error occurred.
     *
     * @return true if should continue; false to throw exception and abort execution
     */
    public boolean isContinueBatchOnError() {
        return continueBatch;
    }

    /**
     * Gets default fetch size for query.
     *
     * @return default fetch size for query
     */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * Checks whether to use local file for INFILE/OUTFILE.
     *
     * @return true to use local file for INFILE/OUTFILE; false otherwise
     */
    public boolean useLocalFile() {
        return localFile;
    }

    /**
     * Gets database term.
     *
     * @return non-null database term
     */
    public String getDatabaseTerm() {
        return databaseTerm;
    }

    /**
     * Checks whether to use catalog as synonymous to database.
     *
     * @return true if use catalog as synonymous to database; false otherwise
     */
    public boolean useCatalog() {
        return TERM_CATALOG.equals(databaseTerm);
    }

    /**
     * Checks whether to use schema as synonymous to database.
     *
     * @return true if use schema as synonymous to database; false otherwise
     */
    public boolean useSchema() {
        return TERM_SCHEMA.equals(databaseTerm);
    }

    /**
     * Gets JDBC dialect.
     *
     * @return non-null JDBC dialect
     */
    public JdbcTypeMapping getDialect() {
        return dialect;
    }

    /**
     * Checks whether external database is supported or not.
     *
     * @return true if external database is supported; false otherwise
     */
    public boolean isExternalDatabaseSupported() {
        return externalDatabase;
    }

    /**
     * Gets custom type map.
     *
     * @return non-null custom type map
     */
    public Map<String, Class<?>> getTypeMap() {
        return typeMap;
    }

    /**
     * Checks whether JDBC-complaint mode is enabled or not.
     *
     * @return true if JDBC-complaint mode is enabled; false otherwise
     */
    public boolean isJdbcCompliant() {
        return jdbcCompliant;
    }

    /**
     * Checks whether transaction support is enabled or not.
     *
     * @return true if transaction support is enabled; false otherwise
     */
    public boolean isTransactionSupported() {
        return txSupport;
    }

    /**
     * Gets default approach to handle null value.
     *
     * @return 0 or negative to throw exception, 1 to disable the null-check, and 2
     *         to reset null to default value of corresponding data type
     */
    public int getNullAsDefault() {
        return nullAsDefault;
    }

    /**
     * Checks whether named parameter should be used instead of JDBC standard
     * question mark placeholder.
     *
     * @return true if named parameter should be used; false otherwise
     */
    public boolean useNamedParameter() {
        return namedParameter;
    }

    /**
     * Checks whether {@link java.sql.Array} and {@link java.sql.Struct} should be
     * returned for array and tuple when calling
     * {@link java.sql.ResultSet#getObject(int)}.
     *
     * @return true if wrapper object should be returned instead of array / tuple;
     *         false otherwise
     */
    public boolean useWrapperObject() {
        return wrapperObject;
    }

    /**
     * Gets properties.
     *
     * @return non-null properties
     */
    public Properties getProperties() {
        Properties props = new Properties();
        props.putAll(this.properties);
        return props;
    }
}
