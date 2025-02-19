package com.clickhouse.jdbc;

import java.io.Serializable;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.jdbc.internal.ClickHouseConnectionImpl;
import com.clickhouse.jdbc.internal.ClickHouseJdbcUrlParser;

/**
 * JDBC driver for ClickHouse. It takes a connection string like below for
 * connecting to ClickHouse server:
 * {@code jdbc:(ch|clickhouse)[:<protocol>]://[<user>[:<password>]@]<host>[:<port>][/<db>][?<parameter=value>,[<parameter=value]]][#<tag>[,<tag>]]}
 *
 * <p>
 * For examples:
 * <ul>
 * <li>{@code jdbc:clickhouse://localhost:8123/system}</li>
 * <li>{@code jdbc:clickhouse://admin:password@localhost/system?socket_time=30}</li>
 * <li>{@code jdbc:clickhouse://localhost/system?protocol=grpc}</li>
 * </ul>
 */
@Deprecated
public class DriverV1 implements Driver {
    private static final Logger log = LoggerFactory.getLogger(DriverV1.class);

    private static final Map<Object, ClickHouseOption> clientSpecificOptions;

    static final String driverVersionString;
    static final ClickHouseVersion driverVersion;
    static final ClickHouseVersion specVersion;

    static final java.util.logging.Logger parentLogger = java.util.logging.Logger.getLogger("com.clickhouse.jdbc");

    public static String frameworksDetected = null;

    public static class FrameworksDetection {
        private static final List<String> FRAMEWORKS_TO_DETECT = Arrays.asList("apache.spark");
        static volatile String frameworksDetected = null;

        private FrameworksDetection() {
        }
        public static String getFrameworksDetected() {
            if (frameworksDetected == null) {
                Set<String> inferredFrameworks = new LinkedHashSet<>();
                for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
                    for (String framework : FRAMEWORKS_TO_DETECT) {
                        if (ste.toString().contains(framework)) {
                            inferredFrameworks.add(String.format("(%s)", framework));
                        }
                    }
                }

                frameworksDetected = String.join("; ", inferredFrameworks);
            }
            return frameworksDetected;
        }
    }

    static {
        log.debug("Initializing ClickHouse JDBC driver V1");
        String str = DriverV1.class.getPackage().getImplementationVersion();
        if (str != null && !str.isEmpty()) {
            char[] chars = str.toCharArray();
            for (int i = 0, len = chars.length; i < len; i++) {
                if (Character.isDigit(chars[i])) {
                    str = str.substring(i);
                    break;
                }
            }
            driverVersionString = str;
        } else {
            driverVersionString = "";
        }
        driverVersion = ClickHouseVersion.of(driverVersionString);
        specVersion = ClickHouseVersion.of(DriverV1.class.getPackage().getSpecificationVersion());

        // client-specific options
        Map<Object, ClickHouseOption> m = new LinkedHashMap<>();
        try {
            for (ClickHouseClient c : ServiceLoader.load(ClickHouseClient.class,
                    DriverV1.class.getClassLoader())) {
                Class<? extends ClickHouseOption> clazz = c.getOptionClass();
                if (clazz == null || clazz == ClickHouseClientOption.class) {
                    continue;
                }
                for (ClickHouseOption o : clazz.getEnumConstants()) {
                    m.put(o.getKey(), o);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to load client-specific options", e);
        }

        clientSpecificOptions = Collections.unmodifiableMap(m);
    }

    public static void load() {
        try {
            log.info("Registering ClickHouse JDBC driver v1 ({})", driverVersion);
            DriverManager.registerDriver(new DriverV1());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }

        log.debug("ClickHouse Driver {}(JDBC: {}) registered", driverVersion, specVersion);
    }

    public static void unload() {
        try {
            log.info("Unregistering ClickHouse JDBC driver v1 ({})", driverVersion);
            DriverManager.deregisterDriver(new DriverV1());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Map<ClickHouseOption, Serializable> toClientOptions(Properties props) {
        if (props == null || props.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<ClickHouseOption, Serializable> options = new HashMap<>();
        for (Entry<Object, Object> e : props.entrySet()) {
            if (e.getKey() == null || e.getValue() == null) {
                continue;
            }

            String key = e.getKey().toString();
            ClickHouseOption o = ClickHouseClientOption.fromKey(key);
            if (o == null) {
                o = clientSpecificOptions.get(key);
            }

            if (o != null) {
                options.put(o, ClickHouseOption.fromString(e.getValue().toString(), o.getValueType()));
            }
        }
        return options;
    }

    private DriverPropertyInfo create(ClickHouseOption option, Properties props) {
        DriverPropertyInfo propInfo = new DriverPropertyInfo(option.getKey(),
                props.getProperty(option.getKey(), String.valueOf(option.getEffectiveDefaultValue())));
        propInfo.required = false;
        propInfo.description = option.getDescription();
        propInfo.choices = null;

        Class<?> clazz = option.getValueType();
        if (Boolean.class == clazz || boolean.class == clazz) {
            propInfo.choices = new String[]{"true", "false"};
        } else if (clazz.isEnum()) {
            Object[] values = clazz.getEnumConstants();
            String[] names = new String[values.length];
            int index = 0;
            for (Object v : values) {
                names[index++] = ((Enum<?>) v).name();
            }
            propInfo.choices = names;
        }
        return propInfo;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && (url.startsWith(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX)
                || url.startsWith(ClickHouseJdbcUrlParser.JDBC_ABBREVIATION_PREFIX));
    }

    @Override
    public ClickHouseConnection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        log.debug("Creating connection");
        return new ClickHouseConnectionImpl(url, info);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        try {
            info = ClickHouseJdbcUrlParser.parse(url, info).getProperties();
        } catch (Exception e) {
            log.error("Could not parse url {}", url, e);
        }

        List<DriverPropertyInfo> result = new ArrayList<>(ClickHouseClientOption.values().length * 2);
        for (ClickHouseClientOption option : ClickHouseClientOption.values()) {
            result.add(create(option, info));
        }

        // and then client-specific options
        for (ClickHouseOption option : clientSpecificOptions.values()) {
            result.add(create(option, info));
        }

        result.addAll(JdbcConfig.getDriverProperties());
        return result.toArray(new DriverPropertyInfo[0]);
    }

    @Override
    public int getMajorVersion() {
        return driverVersion.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return driverVersion.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return parentLogger;
    }
}
