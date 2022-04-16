package com.clickhouse.benchmark.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import com.clickhouse.benchmark.BaseState;
import com.clickhouse.benchmark.Constants;
import com.clickhouse.benchmark.ServerState;
// import com.github.housepower.settings.ClickHouseDefines;

@State(Scope.Thread)
public class DriverState extends BaseState {
    @Param(value = { "clickhouse-jdbc", "clickhouse-grpc-jdbc", "clickhouse-legacy-jdbc", "clickhouse4j",
            "clickhouse-native-jdbc", "mariadb-java-client", "mysql-connector-java", "postgresql-jdbc" })
    private String client;

    @Param(value = { Constants.REUSE_CONNECTION, Constants.NEW_CONNECTION })
    private String connection;

    @Param(value = { Constants.NORMAL_STATEMENT, Constants.PREPARED_STATEMENT })
    private String statement;

    @Param(value = { "default", "string", "object" })
    private String type;

    private Driver driver;
    private String url;
    private Connection conn;

    private int randomSample;
    private int randomNum;

    @Setup(Level.Trial)
    public void doSetup(ServerState serverState) throws Exception {
        JdbcDriver jdbcDriver = JdbcDriver.from(client);

        String compression = String.valueOf(Boolean.parseBoolean(System.getProperty("compression", "true")));
        String additional = System.getProperty("additional", "");

        try {
            driver = (java.sql.Driver) Class.forName(jdbcDriver.getClassName()).getDeclaredConstructor().newInstance();
            url = String.format(jdbcDriver.getUrlTemplate(), serverState.getHost(),
                    serverState.getPort(jdbcDriver.getDefaultPort()), serverState.getDatabase(), serverState.getUser(),
                    serverState.getPassword(), compression, additional);
            // ClickHouseDefines.WRITE_COMPRESS = false;
            // ClickHouseDefines.READ_DECOMPRESS = Boolean.parseBoolean(compression);
            Properties props = new Properties();
            if (jdbcDriver.getClassName().startsWith("com.clickhouse.jdbc.")) {
                props.setProperty("format", System.getProperty("format", "RowBinaryWithNamesAndTypes"));
            }
            conn = driver.connect(url, props);

            try (Statement s = conn.createStatement()) {
                // s.execute("drop table if exists system.test_insert");
                s.execute("truncate table if exists system.test_insert");
                s.execute(
                        "create table if not exists system.test_insert(b String, i Nullable(UInt64), s Nullable(String), t Nullable(DateTime))engine=Memory");
            }

            if (!Constants.REUSE_CONNECTION.equalsIgnoreCase(connection)) {
                conn.close();
                conn = null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @TearDown(Level.Trial)
    public void doTearDown(ServerState serverState) throws SQLException {
        dispose();

        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    @Setup(Level.Iteration)
    public void prepare() {
        if (!Constants.REUSE_CONNECTION.equalsIgnoreCase(connection)) {
            try {
                conn = driver.connect(url, new Properties());
            } catch (SQLException e) {
                throw new IllegalStateException("Failed to create new connection", e);
            }
        }

        randomSample = getRandomNumber(Constants.SAMPLE_SIZE);
        randomNum = getRandomNumber(Constants.FLOATING_RANGE);
    }

    @TearDown(Level.Iteration)
    public void shutdown() {
        if (!Constants.REUSE_CONNECTION.equalsIgnoreCase(connection)) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new IllegalStateException("Failed to close connection", e);
            } finally {
                conn = null;
            }
        }
    }

    public int getSampleSize() {
        return Constants.SAMPLE_SIZE;
    }

    public int getRandomSample() {
        return randomSample;
    }

    public int getRandomNumber() {
        return randomNum;
    }

    public Connection getConnection() throws SQLException {
        return conn;
    }

    public boolean usePreparedStatement() {
        return Constants.PREPARED_STATEMENT.equalsIgnoreCase(this.statement);
    }

    public ConsumeValueFunction getConsumeFunction(ConsumeValueFunction defaultFunc) {
        if ("string".equals(type)) {
            return (b, r, l, i) -> b.consume(r.getString(i));
        } else if ("object".equals(type)) {
            return (b, r, l, i) -> b.consume(r.getObject(i));
        } else if (defaultFunc == null) {
            return (b, r, l, i) -> b.consume(i);
        } else {
            return defaultFunc;
        }
    }

    public SupplyValueFunction getSupplyFunction(SupplyValueFunction defaultFunc) {
        if ("string".equals(type)) {
            return (p, v, l, i) -> p.setString(i, v != null ? v.toString() : null);
        } else if ("object".equals(type)) {
            return (p, v, l, i) -> p.setObject(i, v);
        } else if (defaultFunc == null) {
            return (p, v, l, i) -> p.setObject(i, v);
        } else {
            return defaultFunc;
        }
    }
}
