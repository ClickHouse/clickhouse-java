package tech.clickhouse.benchmark;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Thread)
public class ClientState {
    // sample size used in 10k query/insert
    private final int SAMPLE_SIZE = Integer.parseInt(System.getProperty("sampleSize", "10000"));
    // floating range(to reduce server-side cache hits) used in 10k query/insert
    private final int FLOATING_RANGE = Integer.parseInt(System.getProperty("floatingRange", "100"));

    @Param(value = { "clickhouse4j", "clickhouse-jdbc", "clickhouse-native-jdbc-shaded", "mariadb-java-client",
            "mysql-connector-java", "postgresql-jdbc" })
    private String client;

    @Param(value = { Constants.REUSE_CONNECTION, Constants.NEW_CONNECTION })
    private String connection;

    @Param(value = { Constants.NORMAL_STATEMENT, Constants.PREPARED_STATEMENT })
    private String statement;

    private Driver driver;
    private String url;
    private Connection conn;

    private int randomSample;
    private int randomNum;

    @Setup(Level.Trial)
    public void doSetup(ServerState serverState) throws Exception {
        JdbcDriver jdbcDriver = JdbcDriver.from(client);

        try {
            driver = (java.sql.Driver) Class.forName(jdbcDriver.getClassName()).getDeclaredConstructor().newInstance();
            url = String.format(jdbcDriver.getUrlTemplate(), serverState.getHost(),
                    serverState.getPort(jdbcDriver.getDefaultPort()), serverState.getDatabase(), serverState.getUser(),
                    serverState.getPassword());
            conn = driver.connect(url, new Properties());

            try (Statement s = conn.createStatement()) {
                s.execute("truncate table if exists system.test_insert");
                s.execute(
                        "create table if not exists system.test_insert(i Nullable(UInt64), s Nullable(String), t Nullable(DateTime))engine=Memory");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @TearDown(Level.Trial)
    public void doTearDown(ServerState serverState) throws SQLException {
        if (conn != null) {
            conn.close();
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

        Random random = new Random();
        randomSample = random.nextInt(SAMPLE_SIZE);
        randomNum = random.nextInt(FLOATING_RANGE);
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
        return SAMPLE_SIZE;
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
}
