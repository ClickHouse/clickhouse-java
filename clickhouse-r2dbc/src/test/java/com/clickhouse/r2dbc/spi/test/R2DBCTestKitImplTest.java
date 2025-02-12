package com.clickhouse.r2dbc.spi.test;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.jdbc.ClickHouseDriver;
import com.zaxxer.hikari.HikariDataSource;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.test.TestKit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Optional;
import java.util.TimeZone;

import static com.clickhouse.client.ClickHouseServerForTest.getClickHouseAddress;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class R2DBCTestKitImplTest implements TestKit<String> {

    private static final String DATABASE = "default";
    private static final String USER = "default";
    private static final String PASSWORD = "test_default_password";

    private static final String CUSTOM_PROTOCOL_NAME = System.getProperty("protocol", "http").toUpperCase();
    private static final ClickHouseProtocol DEFAULT_PROTOCOL = ClickHouseProtocol
            .valueOf(CUSTOM_PROTOCOL_NAME.indexOf("HTTP") >= 0 ? "HTTP" : CUSTOM_PROTOCOL_NAME);
    private static final String EXTRA_PARAM = CUSTOM_PROTOCOL_NAME.indexOf("HTTP") >= 0
            && !"HTTP".equals(CUSTOM_PROTOCOL_NAME) ? "http_connection_provider=" + CUSTOM_PROTOCOL_NAME : "";

    static ConnectionFactory connectionFactory;
    static JdbcTemplate jdbcTemplate;

    @BeforeAll
    public static void setup() throws Exception {
        ClickHouseServerForTest.beforeSuite();

        connectionFactory = ConnectionFactories.get(
                format("r2dbc:clickhouse:%s://%s:%s@%s/%s?falan=filan&%s#tag1", DEFAULT_PROTOCOL, USER, PASSWORD,
                        getClickHouseAddress(DEFAULT_PROTOCOL, false), DATABASE, EXTRA_PARAM));
        jdbcTemplate = jdbcTemplate(null);
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    @Override
    public String getPlaceholder(int i) {
        return ":param" + i;
    }

    @Override
    public String getIdentifier(int i) {
        return "param" + i;
    }

    @Override
    public JdbcOperations getJdbcOperations() {
        return jdbcTemplate;
    }

    private static JdbcTemplate jdbcTemplate(String database) throws SQLException {
        HikariDataSource source = new HikariDataSource();

        Driver driver = new ClickHouseDriver();
        DriverManager.registerDriver(driver);
        if (database == null) {
            source.setJdbcUrl(format("jdbc:clickhouse:%s://%s?%s", DEFAULT_PROTOCOL,
                    getClickHouseAddress(DEFAULT_PROTOCOL, false), EXTRA_PARAM));
        } else {
            source.setJdbcUrl(format("jdbc:clickhouse:%s://%s/%s?%s", DEFAULT_PROTOCOL,
                    getClickHouseAddress(DEFAULT_PROTOCOL, false), DATABASE, EXTRA_PARAM));
        }

        source.setUsername(USER);
        source.setPassword(Optional.ofNullable(PASSWORD)
                .map(Object::toString).orElse(null));
        source.setMaximumPoolSize(1);
        source.setConnectionTimeout(Optional.ofNullable(Duration.ofSeconds(5))
                .map(Duration::toMillis).orElse(0L));

        ZoneId zoneId = ZoneId.systemDefault();
        source.addDataSourceProperty("serverTimezone", TimeZone.getTimeZone(zoneId).getID());

        return new JdbcTemplate(source);
    }

    @Override
    @Test
    public void blobInsert() {
        Flux.usingWhen(getConnectionFactory().create(),
                connection -> {

                    Statement statement = connection
                            .createStatement(expand(TestStatement.INSERT_VALUE_PLACEHOLDER, getPlaceholder(0)));
                    assertThrows(IllegalArgumentException.class,
                            () -> statement.bind(0,
                                    Blob.from(Mono.just(ByteBuffer.wrap("Unsupported type".getBytes())))),
                            "bind(0, Blob) should fail");
                    return Mono.empty();
                },
                Connection::close)
                .as(StepVerifier::create)
                .verifyComplete();
    }

    @Override
    @Test
    public void clobInsert() {
        Flux.usingWhen(getConnectionFactory().create(),
                connection -> {

                    Statement statement = connection
                            .createStatement(expand(TestStatement.INSERT_VALUE_PLACEHOLDER, getPlaceholder(0)));
                    assertThrows(IllegalArgumentException.class,
                            () -> statement.bind(0, Clob.from(Mono.just("Unsupported type"))),
                            "bind(0, Clob) should fail");
                    return Mono.empty();
                },
                Connection::close)
                .as(StepVerifier::create)
                .verifyComplete();
    }

    @Override
    @Disabled
    public void blobSelect() {
        // not supported
    }

    @Override
    @Disabled
    public void clobSelect() {
        // not supported
    }

    @Override
    @Test
    public void columnMetadata() {
        getJdbcOperations().execute(expand(TestStatement.INSERT_TWO_COLUMNS));

        Flux.usingWhen(getConnectionFactory().create(),
                connection -> Flux.from(connection

                        .createStatement(expand(TestStatement.SELECT_VALUE_TWO_COLUMNS))
                        .execute()),
                Connection::close)
                .as(StepVerifier::create)
                .expectErrorMatches(ClickHouseException.class::isInstance)
                .verify();
    }

    @Override
    @Test
    // TODO: check if it is doable.
    public void compoundStatement() {
        // compound statements are not supported by clickhouse.
        getJdbcOperations().execute(expand(TestStatement.INSERT_VALUE100));

        Flux.usingWhen(getConnectionFactory().create(),
                connection -> Flux.from(connection

                        .createStatement(expand(TestStatement.SELECT_VALUE_BATCH))
                        .execute()),
                Connection::close)
                .as(StepVerifier::create)
                .expectErrorMatches(ClickHouseException.class::isInstance)
                .verify();
    }

    @Override
    @Test
    public void duplicateColumnNames() {
        getJdbcOperations().execute(expand(TestStatement.INSERT_TWO_COLUMNS));

        Flux.usingWhen(getConnectionFactory().create(),
                connection -> Flux.from(connection

                        .createStatement(expand(TestStatement.SELECT_VALUE_TWO_COLUMNS))
                        .execute()),
                Connection::close)

                .as(StepVerifier::create)
                .expectErrorMatches(ClickHouseException.class::isInstance)
                .verify();
    }

    @Override
    @Test
    @Disabled
    public void returnGeneratedValues() {
        // not supported
    }

    @Override
    @Test
    @Disabled
    public void returnGeneratedValuesFails() {
        // not supported
    }

    @Override
    @Test
    @Disabled
    public void transactionRollback() {
        // since there is not transaction support, this test case is disabled.
    }

    @Override
    @Test
    @Disabled
    public void sameAutoCommitLeavesTransactionUnchanged() {
        // since there is not transaction support, this test case is disabled.
    }

    @Override
    @Test
    @Disabled
    public void savePoint() {

    }

    @Override
    @Test
    @Disabled
    public void savePointStartsTransaction() {

    }

    @Override
    public String expand(TestStatement statement, Object... args) {
        try {
            String sql = ClickHouseTestStatement.get(statement).getSql();
            return String.format(sql, args);
        } catch (IllegalArgumentException e) {
            return String.format(statement.getSql(), args);
        }
    }

    private enum ClickHouseTestStatement {
        CREATE_TABLE(TestStatement.CREATE_TABLE, "CREATE TABLE test ( test_value INTEGER ) ENGINE = Memory"),
        CREATE_TABLE_TWO_COLUMNS(TestStatement.CREATE_TABLE_TWO_COLUMNS,
                "CREATE TABLE test_two_column ( col1 INTEGER, col2 VARCHAR(100) ) ENGINE = Memory"),
        CREATE_BLOB_TABLE(TestStatement.CREATE_BLOB_TABLE, "CREATE TABLE blob_test ( test_value %s ) ENGINE = Memory"),
        CREATE_CLOB_TABLE(TestStatement.CREATE_CLOB_TABLE, "CREATE TABLE clob_test ( test_value %s ) ENGINE = Memory"),
        CREATE_TABLE_AUTOGENERATED_KEY(TestStatement.CREATE_TABLE_AUTOGENERATED_KEY,
                "CREATE TABLE test ( id DATE DEFAULT toDate(now()) ,  test_value INTEGER ) ENGINE = Memory"),
        INSERT_VALUE_AUTOGENERATED_KEY(TestStatement.INSERT_VALUE_AUTOGENERATED_KEY,
                "INSERT INTO test(test_value) VALUES(100)");

        ClickHouseTestStatement(TestStatement testStatement, String sql) {
            this.testStatementToBeOverwridden = testStatement;
            this.sql = sql;
        }

        TestStatement testStatementToBeOverwridden;
        String sql;

        static ClickHouseTestStatement get(TestStatement testStatement) {
            for (ClickHouseTestStatement cts : values()) {
                if (cts.getTestStatementToBeOverwridden() == testStatement)
                    return cts;
            }
            throw new IllegalArgumentException("Teststatement is not found.");
        }

        public String getSql() {
            return sql;
        }

        public TestStatement getTestStatementToBeOverwridden() {
            return testStatementToBeOverwridden;
        }
    }
}
