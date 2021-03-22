package tech.clickhouse.benchmark;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Thread)
public class ClientState {
    @Param(value = { "clickhouse4j", Constants.CLICKHOUSE_DRIVER, "clickhouse-native-jdbc-shaded",
            "mariadb-java-client", "mysql-connector-java" })
    private String client;

    @Param(value = { Constants.NORMAL_STATEMENT, Constants.PREPARED_STATEMENT })
    private String statement;

    private Connection conn;

    @Setup(Level.Trial)
    public void doSetup(ServerState serverState) throws Exception {
        JdbcDriver driver = JdbcDriver.from(client);

        try {
            conn = ((java.sql.Driver) Class.forName(driver.getClassName()).getDeclaredConstructor().newInstance())
                    .connect(String.format(driver.getUrlTemplate(), serverState.getHost(),
                            serverState.getPort(driver.getDefaultPort()), serverState.getDatabase(),
                            serverState.getUser(), serverState.getPassword()), new Properties());

            try (Statement s = conn.createStatement()) {
                s.execute(
                        "create table if not exists test_insert(i Nullable(UInt64), s Nullable(String), t Nullable(DateTime))engine=Memory");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @TearDown(Level.Trial)
    public void doTearDown() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_insert");
        }
        conn.close();
    }

    public Connection getConnection() {
        return this.conn;
    }

    public boolean usePreparedStatement() {
        return Constants.PREPARED_STATEMENT.equals(this.statement);
    }
}
