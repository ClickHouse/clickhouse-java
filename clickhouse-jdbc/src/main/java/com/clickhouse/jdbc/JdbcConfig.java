package com.clickhouse.jdbc;

import java.sql.DriverPropertyInfo;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class JdbcConfig {
    public static final String PROP_AUTO_COMMIT = "autoCommit";
    public static final String PROP_FETCH_SIZE = "fetchSize";
    public static final String PROP_JDBC_COMPLIANT = "jdbcCompliant";
    public static final String PROP_NAMED_PARAM = "namedParameter";
    public static final String PROP_WRAPPER_OBJ = "wrapperObject";

    private static final String BOOLEAN_FALSE = "false";
    private static final String BOOLEAN_TRUE = "true";

    private static final String DEFAULT_AUTO_COMMIT = BOOLEAN_TRUE;
    private static final String DEFAULT_FETCH_SIZE = "0";
    private static final String DEFAULT_JDBC_COMPLIANT = BOOLEAN_TRUE;
    private static final String DEFAULT_NAMED_PARAM = BOOLEAN_FALSE;
    private static final String DEFAULT_WRAPPER_OBJ = BOOLEAN_FALSE;

    static boolean extractBooleanValue(Properties props, String key, String defaultValue) {
        if (props == null || props.isEmpty() || key == null || key.isEmpty()) {
            return Boolean.parseBoolean(defaultValue);
        }

        Object value = props.remove(key);
        return Boolean.parseBoolean(value != null ? value.toString() : defaultValue);
    }

    static int extractIntValue(Properties props, String key, String defaultValue) {
        if (props == null || props.isEmpty() || key == null || key.isEmpty()) {
            return Integer.parseInt(defaultValue);
        }

        Object value = props.remove(key);
        return Integer.parseInt(value != null ? value.toString() : defaultValue);
    }

    public static List<DriverPropertyInfo> getDriverProperties() {
        List<DriverPropertyInfo> list = new LinkedList<>();
        DriverPropertyInfo info = new DriverPropertyInfo(PROP_AUTO_COMMIT, DEFAULT_AUTO_COMMIT);
        info.choices = new String[] { BOOLEAN_TRUE, BOOLEAN_FALSE };
        info.description = "Whether to enable auto commit when connection is created.";
        list.add(info);

        info = new DriverPropertyInfo(PROP_FETCH_SIZE, DEFAULT_FETCH_SIZE);
        info.description = "Default fetch size, negative or zero means no preferred option.";
        list.add(info);

        info = new DriverPropertyInfo(PROP_JDBC_COMPLIANT, DEFAULT_JDBC_COMPLIANT);
        info.choices = new String[] { BOOLEAN_TRUE, BOOLEAN_FALSE };
        info.description = "Whether to enable JDBC-compliant features like fake transaction and standard UPDATE and DELETE statements.";
        list.add(info);

        info = new DriverPropertyInfo(PROP_NAMED_PARAM, DEFAULT_NAMED_PARAM);
        info.choices = new String[] { BOOLEAN_TRUE, BOOLEAN_FALSE };
        info.description = "Whether to use named parameter(e.g. :ts(DateTime64(6)) or :value etc.) instead of standard JDBC question mark placeholder.";
        list.add(info);

        info = new DriverPropertyInfo(PROP_WRAPPER_OBJ, DEFAULT_WRAPPER_OBJ);
        info.choices = new String[] { BOOLEAN_TRUE, BOOLEAN_FALSE };
        info.description = "Whether to return wrapper object like Array or Struct in ResultSet.getObject method.";
        list.add(info);

        return Collections.unmodifiableList(list);
    }

    private final boolean autoCommit;
    private final int fetchSize;
    private final boolean jdbcCompliant;
    private final boolean namedParameter;
    private final boolean wrapperObject;

    public JdbcConfig() {
        this(null);
    }

    public JdbcConfig(Properties props) {
        if (props == null) {
            props = new Properties();
        }

        this.autoCommit = extractBooleanValue(props, PROP_AUTO_COMMIT, DEFAULT_AUTO_COMMIT);
        this.fetchSize = extractIntValue(props, PROP_FETCH_SIZE, DEFAULT_FETCH_SIZE);
        this.jdbcCompliant = extractBooleanValue(props, PROP_JDBC_COMPLIANT, DEFAULT_JDBC_COMPLIANT);
        this.namedParameter = extractBooleanValue(props, PROP_NAMED_PARAM, DEFAULT_NAMED_PARAM);
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
     * Gets default fetch size for query.
     *
     * @return default fetch size for query
     */
    public int getFetchSize() {
        return fetchSize;
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
}
